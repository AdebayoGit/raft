/// Field-level document codec — raft's compact binary wire format, exposed
/// as a tiny builder/reader pair so typed collections need **no code
/// generation**: you write one `encode` and one `decode` closure per type
/// and never touch bytes or JSON.
///
/// Wire format (little-endian, mirrors `core/src/codec.rs`):
///
/// ```text
/// batch := u32 doc_count, doc_count × (u32 doc_len, doc)
/// doc   := u64 id, u16 field_count, field_count × field
/// field := u8 name_len, name (UTF-8), u8 tag, payload
/// tags  := 0 Null | 1 Bool | 2 Int(i64) | 3 Float(f64)
///        | 4 String(u32+utf8) | 5 Bytes(u32+raw)
/// ```
library;

import 'dart:convert';
import 'dart:typed_data';

const _tagNull = 0;
const _tagBool = 1;
const _tagInt = 2;
const _tagFloat = 3;
const _tagString = 4;
const _tagBytes = 5;

/// Collects one document's fields. Reused across documents by the
/// collection layer, so encoding a batch allocates once for the output —
/// not once per field.
///
/// ```dart
/// encode: (todo, w) => w
///   ..string('title', todo.title)
///   ..boolean('done', todo.done)
///   ..integer('priority', todo.priority),
/// ```
class RaftDocWriter {
  final List<_Field> _fields = [];

  /// Add a UTF-8 string field.
  void string(String name, String value) =>
      _fields.add(_Field(name, _tagString, utf8.encode(value)));

  /// Add a 64-bit integer field.
  void integer(String name, int value) =>
      _fields.add(_Field(name, _tagInt, value));

  /// Add a double field.
  void float(String name, double value) =>
      _fields.add(_Field(name, _tagFloat, value));

  /// Add a boolean field.
  void boolean(String name, bool value) =>
      _fields.add(_Field(name, _tagBool, value));

  /// Add a raw bytes field.
  void bytes(String name, Uint8List value) =>
      _fields.add(_Field(name, _tagBytes, value));

  /// Add an explicit null field (a present-but-empty marker).
  void nullField(String name) => _fields.add(_Field(name, _tagNull, null));

  void _reset() => _fields.clear();

  int _size() {
    var total = 8 + 2; // id + field count
    for (final f in _fields) {
      total += 1 + f.nameBytes.length + 1; // name_len + name + tag
      switch (f.tag) {
        case _tagNull:
          break;
        case _tagBool:
          total += 1;
        case _tagInt || _tagFloat:
          total += 8;
        case _tagString || _tagBytes:
          total += 4 + (f.value as List<int>).length;
      }
    }
    return total;
  }

  int _writeInto(Uint8List out, ByteData bd, int o, int id) {
    bd.setUint64(o, id, Endian.little);
    o += 8;
    bd.setUint16(o, _fields.length, Endian.little);
    o += 2;
    for (final f in _fields) {
      out[o++] = f.nameBytes.length;
      out.setAll(o, f.nameBytes);
      o += f.nameBytes.length;
      out[o++] = f.tag;
      switch (f.tag) {
        case _tagNull:
          break;
        case _tagBool:
          out[o++] = (f.value as bool) ? 1 : 0;
        case _tagInt:
          bd.setInt64(o, f.value as int, Endian.little);
          o += 8;
        case _tagFloat:
          bd.setFloat64(o, f.value as double, Endian.little);
          o += 8;
        case _tagString || _tagBytes:
          final b = f.value as List<int>;
          bd.setUint32(o, b.length, Endian.little);
          o += 4;
          out.setAll(o, b);
          o += b.length;
      }
    }
    return o;
  }
}

class _Field {
  _Field(String name, this.tag, this.value) : nameBytes = utf8.encode(name);
  final Uint8List nameBytes;
  final int tag;
  final Object? value;
}

/// Random-access view of one decoded document. Field lookups are by name;
/// missing fields throw a [StateError] with the field and collection
/// context so typos fail loudly, not with silent zeros.
class RaftDocReader {
  RaftDocReader._(this.id, this._values);

  /// The document's primary key.
  final int id;
  final Map<String, Object?> _values;

  T _get<T>(String name) {
    final v = _values[name];
    if (v is T) return v;
    if (!_values.containsKey(name)) {
      throw StateError(
        'field "$name" not present in document $id — stored fields: '
        '${_values.keys.join(", ")}',
      );
    }
    throw StateError(
      'field "$name" in document $id is ${v.runtimeType}, not $T',
    );
  }

  String string(String name) => _get<String>(name);
  int integer(String name) => _get<int>(name);
  double float(String name) => _get<double>(name);
  bool boolean(String name) => _get<bool>(name);
  Uint8List bytes(String name) => _get<Uint8List>(name);

  String? stringOrNull(String name) => _values[name] as String?;
  int? integerOrNull(String name) => _values[name] as int?;
  double? floatOrNull(String name) => _values[name] as double?;
  bool? booleanOrNull(String name) => _values[name] as bool?;

  /// Whether the document stores [name] (including as an explicit null).
  bool has(String name) => _values.containsKey(name);
}

/// Batch/document (de)serialisation used by the collection layer. Not part
/// of the public API surface.
class RaftWire {
  /// Encode [count] documents via [encodeAt] into one batch buffer.
  static Uint8List encodeBatch(
    int count,
    void Function(int index, RaftDocWriter w) encodeAt,
    int Function(int index) idAt,
  ) {
    final writer = RaftDocWriter();
    // Pass 1: sizes (fields are captured per doc, so encode closures run
    // once — captured field lists are reused in pass 2).
    final captured = <List<_Field>>[];
    var total = 4;
    for (var i = 0; i < count; i++) {
      writer._reset();
      encodeAt(i, writer);
      captured.add(List.of(writer._fields));
      total += 4 + writer._size();
    }
    final out = Uint8List(total);
    final bd = ByteData.view(out.buffer);
    var o = 0;
    bd.setUint32(o, count, Endian.little);
    o += 4;
    for (var i = 0; i < count; i++) {
      writer._fields
        ..clear()
        ..addAll(captured[i]);
      final size = writer._size();
      bd.setUint32(o, size, Endian.little);
      o += 4;
      o = writer._writeInto(out, bd, o, idAt(i));
    }
    assert(o == total);
    return out;
  }

  /// Encode a single document.
  static Uint8List encodeDoc(int id, void Function(RaftDocWriter w) encode) {
    final writer = RaftDocWriter();
    encode(writer);
    final out = Uint8List(writer._size());
    final bd = ByteData.view(out.buffer);
    final end = writer._writeInto(out, bd, 0, id);
    assert(end == out.length);
    return out;
  }

  /// Decode one document spanning [length] bytes at [offset].
  static RaftDocReader decodeDoc(
    Uint8List buf,
    ByteData bd,
    int offset,
    int length,
  ) {
    var o = offset;
    final end = offset + length;
    final id = bd.getUint64(o, Endian.little);
    o += 8;
    final fieldCount = bd.getUint16(o, Endian.little);
    o += 2;
    final values = <String, Object?>{};
    for (var f = 0; f < fieldCount; f++) {
      if (o >= end) throw const FormatException('truncated document');
      final nameLen = buf[o++];
      final name = utf8.decode(Uint8List.sublistView(buf, o, o + nameLen));
      o += nameLen;
      final tag = buf[o++];
      switch (tag) {
        case _tagNull:
          values[name] = null;
        case _tagBool:
          values[name] = buf[o++] != 0;
        case _tagInt:
          values[name] = bd.getInt64(o, Endian.little);
          o += 8;
        case _tagFloat:
          values[name] = bd.getFloat64(o, Endian.little);
          o += 8;
        case _tagString:
          final len = bd.getUint32(o, Endian.little);
          o += 4;
          values[name] = utf8.decode(Uint8List.sublistView(buf, o, o + len));
          o += len;
        case _tagBytes:
          final len = bd.getUint32(o, Endian.little);
          o += 4;
          values[name] = Uint8List.fromList(
            Uint8List.sublistView(buf, o, o + len),
          );
          o += len;
        default:
          throw FormatException('unknown field tag $tag at offset $o');
      }
    }
    if (o != end) throw const FormatException('trailing bytes in document');
    return RaftDocReader._(id, values);
  }

  /// Decode a batch buffer into readers.
  static List<RaftDocReader> decodeBatch(Uint8List buf) {
    final bd = ByteData.view(buf.buffer, buf.offsetInBytes, buf.length);
    var o = 0;
    final count = bd.getUint32(o, Endian.little);
    o += 4;
    final out = <RaftDocReader>[];
    for (var i = 0; i < count; i++) {
      final len = bd.getUint32(o, Endian.little);
      o += 4;
      out.add(decodeDoc(buf, bd, o, len));
      o += len;
    }
    return out;
  }
}
