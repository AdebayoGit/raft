import 'dart:convert';
import 'dart:typed_data';

import '../model.dart';

/// Dart mirror of raft's binary document codec (`core/src/ffi/codec.rs`).
///
/// ```text
/// batch := u32 doc_count, doc_count × (u32 doc_len, doc)   (little-endian)
/// doc   := u64 id, u16 field_count, field_count × field
/// field := u8 name_len, name, u8 tag, payload
/// tags  := 0 Null | 1 Bool | 2 Int(i64) | 3 Float(f64)
///        | 4 String(u32+utf8) | 5 Bytes(u32+raw)
/// ```
///
/// Hot-path notes: strings are UTF-8-encoded exactly once, decodes use
/// range-based [Utf8Decoder.convert] (no `sublist` copies), and the three
/// BenchDoc field names are dispatched by length (4/5/7 — unique), so no
/// name bytes are ever decoded.
class RaftCodec {
  static const _tagInt = 2;
  static const _tagString = 4;

  static final Uint8List _nameName = ascii.encode('name');
  static final Uint8List _nameScore = ascii.encode('score');
  static final Uint8List _namePayload = ascii.encode('payload');

  /// Encode one [BenchDoc] batch straight to the wire format — no JSON, no
  /// intermediate maps, one allocation for the output.
  static Uint8List encodeBatch(List<BenchDoc> docs, {int scoreDelta = 0}) {
    // Encode every string once; sizes fall out of the encoded lengths.
    final names = List<Uint8List>.generate(
        docs.length, (i) => utf8.encode(docs[i].name),
        growable: false);
    final payloads = List<Uint8List>.generate(
        docs.length, (i) => utf8.encode(docs[i].payload),
        growable: false);

    const fixedPerDoc = 8 + // id
        2 + // field count
        (1 + 4) + 1 + 4 + // "name" + tag + len
        (1 + 5) + 1 + 8 + // "score" + tag + i64
        (1 + 7) + 1 + 4; // "payload" + tag + len
    var total = 4;
    for (var i = 0; i < docs.length; i++) {
      total += 4 + fixedPerDoc + names[i].length + payloads[i].length;
    }

    final out = Uint8List(total);
    final bd = ByteData.view(out.buffer);
    var o = 0;
    bd.setUint32(o, docs.length, Endian.little);
    o += 4;
    for (var i = 0; i < docs.length; i++) {
      final d = docs[i];
      final size = fixedPerDoc + names[i].length + payloads[i].length;
      bd.setUint32(o, size, Endian.little);
      o += 4;

      bd.setUint64(o, d.id, Endian.little);
      o += 8;
      bd.setUint16(o, 3, Endian.little);
      o += 2;

      o = _fieldName(out, o, _nameName);
      out[o++] = _tagString;
      bd.setUint32(o, names[i].length, Endian.little);
      o += 4;
      out.setAll(o, names[i]);
      o += names[i].length;

      o = _fieldName(out, o, _nameScore);
      out[o++] = _tagInt;
      bd.setInt64(o, d.score + scoreDelta, Endian.little);
      o += 8;

      o = _fieldName(out, o, _namePayload);
      out[o++] = _tagString;
      bd.setUint32(o, payloads[i].length, Endian.little);
      o += 4;
      out.setAll(o, payloads[i]);
      o += payloads[i].length;
    }
    assert(o == total);
    return out;
  }

  static int _fieldName(Uint8List out, int o, Uint8List name) {
    out[o++] = name.length;
    out.setAll(o, name);
    return o + name.length;
  }

  static final Utf8Decoder _utf8 = const Utf8Decoder(allowMalformed: false);

  /// Decode a batch, materialising every field of every document — the same
  /// work a competitor's `findAll()` object mapping performs.
  static List<BenchDoc> decodeBatch(Uint8List buf) {
    final bd = ByteData.view(buf.buffer, buf.offsetInBytes, buf.length);
    var o = 0;
    final count = bd.getUint32(o, Endian.little);
    o += 4;
    final docs = List<BenchDoc?>.filled(count, null, growable: false);
    for (var i = 0; i < count; i++) {
      final len = bd.getUint32(o, Endian.little);
      o += 4;
      docs[i] = decodeDoc(buf, bd, o, len);
      o += len;
    }
    return docs.cast<BenchDoc>();
  }

  /// Decode a single document at [offset] spanning [length] bytes.
  ///
  /// Field names are dispatched by length (name=4, score=5, payload=7),
  /// and string payloads decode via range-based UTF-8 — zero intermediate
  /// copies.
  static BenchDoc decodeDoc(
      Uint8List buf, ByteData bd, int offset, int length) {
    var o = offset;
    final id = bd.getUint64(o, Endian.little);
    o += 8;
    final fieldCount = bd.getUint16(o, Endian.little);
    o += 2;
    String name = '';
    int score = 0;
    String payload = '';
    for (var f = 0; f < fieldCount; f++) {
      final nameLen = buf[o++];
      o += nameLen; // names dispatched by length below — never decoded
      final tag = buf[o++];
      switch (tag) {
        case 0: // Null
          break;
        case 1: // Bool
          o += 1;
        case 2: // Int
          final v = bd.getInt64(o, Endian.little);
          o += 8;
          if (nameLen == 5) score = v; // "score"
        case 3: // Float
          o += 8;
        case 4: // String
          final len = bd.getUint32(o, Endian.little);
          o += 4;
          final s = _utf8.convert(buf, o, o + len);
          o += len;
          if (nameLen == 4) {
            name = s; // "name"
          } else if (nameLen == 7) {
            payload = s; // "payload"
          }
        case 5: // Bytes
          final len = bd.getUint32(o, Endian.little);
          o += 4 + len;
        default:
          throw FormatException('unknown tag $tag at $o');
      }
    }
    return BenchDoc(id: id, name: name, score: score, payload: payload);
  }
}
