This README serves to document edge cases discovered while investigating pushdown of $match, $sort,
and $project.

# $match

## Arrays

If a path refers to an array field, we first evaluate the whole array against the match clause. For
inequalities, arrays are compared lexicographically. If the predicate is not matched, we then
attempt to match each element of the array against the clause. We only perform element matching one
level deep. For example:

```bson
{
 array: [1, [2]]
}
```

The above document would match `{array: {$eq: [1, [2]]}}` and `{array: {$eq: 1}}`
but not `{array: {$eq: 2}}`

# $project

In general, $project should be consistent with $match such that if $match and $project appear in the
same pipeline, then the values projected out should be consistent with the $match stage. The order
of
fields returned by $project is the same as they appear in the indexed document -- not in the order
they are declared in the $project stage. A $project definition must have unique keys, otherwise it
is rejected by the server. Projected paths must not be a substring of other projected paths,
otherwise it is rejected by the server.

# Repeated Keys

Repeated keys refers to Bson documents that have duplicate keys at the same level -- not an array.

```bson
{
  foo: 1
  foo: null
}
```

Such documents are valid according to the bson spec, but not all driver implementations allow the
user to create such documents. The server, however, will preserve any repeated keys and has
deterministic rules for how they are handled. Mongot needs to replicate this behavior for pushdown.

This behavior is often undocumented and cannot be tested in Mongosh. However, it can be verified
through the Java driver, for example.

```java
    MongoClient client = MongoClients.create(
    "mongodb://mongoUser:hunter1@localhost:37017/?authSource=admin");
var doc = RawBsonDocument.parse("{a: 1, a: 3.14, foo: {b: 1, b:2}}");
    client.

getDatabase("keys").

getCollection("fts",RawBsonDocument .class).

insertOne(doc);

var cursor = client.getDatabase("keys")
    .getCollection("fts", RawBsonDocument.class)
    .aggregate(List.of(new BsonDocument("$project", new BsonDocument("foo.b", new BsonInt64(0)))));
    System.out.

println(cursor.iterator().

tryNext());
```

## $match

If a document contains repeated keys, only the first value is tested. For example, consider the
following collection:

```bson
[
    {
      foo: 1
      foo: null
    }
]
```

In this case `{$match: {foo: 1}}` should return the document, but `{$match: {foo: null}}` should
not.

## $project

```bson
{
  foo: 1
  foo: null
}
```

### Inclusions

Then `{$project: {foo: 1}}` will return only the first value `{foo: 1}`

### Exclusions

Then `{$project: {foo: 0}}` will exclude only the first value and return `{foo: null}`

## Sorting

[Public documentation](https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/)
for sort order is incomplete. Since the server appears to break ties based on insertion order, we
can deduce sort order by inserting every type twice. This requires use of the Java driver since
Mongosh does not support all types.

Note: $sort brackets do NOT follow the same logic as $match brackets. Also note that `v` and `[v]`
do not share the same sort bracket if v is an array.

```javascript
const values = [MinKey(), MaxKey(), null, 0, [], false, "", {}, new Date(), new BSONRegExp("r"),
  new BSONSymbol(""), new Timestamp(), new DBRef("name", new ObjectId()), new ObjectId(),
  new UUID()]
for (let v of values) {
  db.fts.insertMany({a: v});
}
for (let v of values) {
  db.fts.insertMany({a: v});
}
db.fts.aggregate([{$sort: {a: 1}}, {$project: {_id: 0, a: 1, type: {$type: "$a"}}}]).toArray()
```

```javascript
[
  // Bracket 1
  {a: MinKey(), type: 'minKey'},
  {a: [MinKey()], type: 'array'},
  // Bracket 2
  {a: [], type: 'array'},
  {a: undefined, type: 'undefined'},
  {a: [undefined], type: 'array'},
  // Bracket 3
  {a: null, type: 'null'},
  {a: [null], type: 'array'},
  {type: 'missing'},
  // Bracket 4
  {a: 0, type: 'int'},
  {a: [0], type: 'array'},
  // Bracket 5
  {a: '', type: 'string'},
  {a: [''], type: 'array'},
  {a: '', type: 'symbol'},
  {a: [''], type: 'array'},
  // Bracket 6
  {a: {}, type: 'object'},
  {a: [{}], type: 'array'},
  // Bracket 7
  {
    a: DBRef('name', ObjectId('663152d072552758a26458eb')),
    type: 'object'
  },
  // Bracket 8
  {a: [[]], type: 'array'},
  // Bracket 9
  {a: UUID('57c566d5-04ce-4091-832d-6c20df00da49'), type: 'binData'},
  {
    a: [UUID('57c566d5-04ce-4091-832d-6c20df00da49')],
    type: 'array'
  },
  // Bracket 10
  {a: ObjectId('663152d072552758a26458ec'), type: 'objectId'},
  {a: [ObjectId('663152d072552758a26458ec')], type: 'array'},
  // Bracket 11
  {a: false, type: 'bool'},
  {a: [false], type: 'array'},
  // Bracket 12
  {a: ISODate('2024-04-30T20:21:36.274Z'), type: 'date'},
  {a: [ISODate('2024-04-30T20:21:36.274Z')], type: 'array'},
  // Bracket 13
  {a: Timestamp({t: 1714508577, i: 23}), type: 'timestamp'},
  // Bracket 14
  {a: /r/, type: 'regex'},
  {a: [/r/], type: 'array'},
  // Bracket 15  
  {a: Code(''), type: 'javascript'},
  // Bracket 16
  {a: Code('', {}), type: 'javascriptWithScope'},
  // Bracket 17
  {a: MaxKey(), type: 'maxKey'},
  {a: [MaxKey()], type: 'array'},
]
```

## Nested Arrays

Arrays sort after Objects, but the first level of arrays are always unwrapped so that `[5]` is actually
treated at sorting `5`. The only exception is for empty arrays, which cannot be unwrapped and are
sorted as `MinKey < [] < Undefined`. Otherwise, `BsonArray`'s sort priority is considered when
selecting a representative element from an array. Empirically, unwrapping does not seem to be
recursive. If the representative element of an array is itself an array, then arrays are compared
lexicographically.

```javascript
[
  {a: [MinKey(), []]}, // rep=MinKey
  {a: []},             // rep=_
  {a: [null]},         // rep=null
  {a: [null, []]},     // rep=null 
  {a: [0, []]},        // rep=0
  {a: [false, []]},    // rep=[]
  {a: [[MinKey()]]},   // rep=[MinKey]
  {a: [[null]]},       // rep=[null]
  {a: [[false, MinKey()]]}, // rep=[false, MinKey]
  {a: [false]}         // rep=false 
]
```

