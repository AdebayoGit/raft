// dart format width=80
// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'realm_adapter.dart';

// **************************************************************************
// RealmObjectGenerator
// **************************************************************************

// coverage:ignore-file
// ignore_for_file: type=lint
class RealmDoc extends _RealmDoc
    with RealmEntity, RealmObjectBase, RealmObject {
  RealmDoc(int id, String name, int score, String payload) {
    RealmObjectBase.set(this, 'id', id);
    RealmObjectBase.set(this, 'name', name);
    RealmObjectBase.set(this, 'score', score);
    RealmObjectBase.set(this, 'payload', payload);
  }

  RealmDoc._();

  @override
  int get id => RealmObjectBase.get<int>(this, 'id') as int;
  @override
  set id(int value) => RealmObjectBase.set(this, 'id', value);

  @override
  String get name => RealmObjectBase.get<String>(this, 'name') as String;
  @override
  set name(String value) => RealmObjectBase.set(this, 'name', value);

  @override
  int get score => RealmObjectBase.get<int>(this, 'score') as int;
  @override
  set score(int value) => RealmObjectBase.set(this, 'score', value);

  @override
  String get payload => RealmObjectBase.get<String>(this, 'payload') as String;
  @override
  set payload(String value) => RealmObjectBase.set(this, 'payload', value);

  @override
  Stream<RealmObjectChanges<RealmDoc>> get changes =>
      RealmObjectBase.getChanges<RealmDoc>(this);

  @override
  Stream<RealmObjectChanges<RealmDoc>> changesFor([List<String>? keyPaths]) =>
      RealmObjectBase.getChangesFor<RealmDoc>(this, keyPaths);

  @override
  RealmDoc freeze() => RealmObjectBase.freezeObject<RealmDoc>(this);

  EJsonValue toEJson() {
    return <String, dynamic>{
      'id': id.toEJson(),
      'name': name.toEJson(),
      'score': score.toEJson(),
      'payload': payload.toEJson(),
    };
  }

  static EJsonValue _toEJson(RealmDoc value) => value.toEJson();
  static RealmDoc _fromEJson(EJsonValue ejson) {
    if (ejson is! Map<String, dynamic>) return raiseInvalidEJson(ejson);
    return switch (ejson) {
      {
        'id': EJsonValue id,
        'name': EJsonValue name,
        'score': EJsonValue score,
        'payload': EJsonValue payload,
      } =>
        RealmDoc(
          fromEJson(id),
          fromEJson(name),
          fromEJson(score),
          fromEJson(payload),
        ),
      _ => raiseInvalidEJson(ejson),
    };
  }

  static final schema = () {
    RealmObjectBase.registerFactory(RealmDoc._);
    register(_toEJson, _fromEJson);
    return const SchemaObject(ObjectType.realmObject, RealmDoc, 'RealmDoc', [
      SchemaProperty('id', RealmPropertyType.int, primaryKey: true),
      SchemaProperty('name', RealmPropertyType.string),
      SchemaProperty('score', RealmPropertyType.int),
      SchemaProperty('payload', RealmPropertyType.string),
    ]);
  }();

  @override
  SchemaObject get objectSchema => RealmObjectBase.getSchema(this) ?? schema;
}
