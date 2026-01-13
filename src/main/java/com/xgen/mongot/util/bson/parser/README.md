# BSON Parser

The BSON Parser library provides a fluent, composable API for (de)serializing BSON documents into/from Java objects.

## API Overview

### Fields

Classes should declare `Field`s as static members for each expected field in a BSON document.

`Field`s are constructed using a builder class, which requires supplying the field name, expected type, type-specific validations, and whether the field is required or optional. The values returned by the builders are strongly typed and ensure that you supply all of the required information.

#### Examples


```java
public class Foo {
  
  public static class Fields {

    /* Type specific builders will offer some built in validation options. */
    public static final Field.Required<Integer> MY_INT =
        Field.builder("myInt")
            .intField()
            .mustBePositive()
            .required();

    /* All builders also offer generic validation. */
    public static final Field.Required<String> MY_STRING =
        Field.builder("myString")
            .stringField()
            .validate(
                s ->
                  s.startsWith("the prefix")
                    ? Optional.empty()
                    : Optional.of("must begin with \"the prefix\""))
            .required();
  
    /* Fields can be optional. */
    public static final Field.Optional<String> MY_OTHER_STRING =
        Field.builder("myOtherString")
            .stringField()
            .optional()
            .noDefault();

    /* Optional fields can also have default values. */
    public static final Field.WithDefault<Boolean> MY_BOOL =
        Field.builder("myBool")
            .booleanField()
            .optional()
            .withDefault(true);
  }

  ...
}
```

### Document Parsing

In order to parse your class from BSON you should provide a static factory method with the following signature:

```java
public static Foo fromBson(DocumentParser parser) throws BsonParseException;
```

This method will use `DocumentParser::getField` to extract the fields from the underlying BSON document.

`DocumentParser::getField` will return a `ParsedField` matching the type of `Field` class (e.g. a `ParsedField.Optional<T>` for a `Field.Optional<T>`). This returned value will have an `unwrap()` method that will return the appropriate underlying value. Unless you need to do further validation, such as [ensuring mutually exclusive fields are not provided](#field-groups), you can just call `unwrap()` immediately.

Using our example from above, a class may end up looking like:

```java
public class Foo {

  ...

  private Foo(int myInt, String myString, Optional<String> myOtherString, boolean myBool) {
    ...
  }

  public static Foo fromBson(DocumentParser parser) throws BsonParseException {
    return new Foo(
        parser.getField(Fields.MY_INT).unwrap(),
        parser.getField(Fields.MY_STRING).unwrap(),
        parser.getField(Fields.MY_OTHER_STRING).unwrap(),
        parser.getField(Fields.MY_BOOL).unwrap());;
  }

  ...
}
```

Each call to `DocumentParser::getField` will appropriately validate the requested field and throw a detailed `BsonParseException` if validation fails.

For example, if the `"myInt"` field was not present it would throw a `BsonParseException` with the message `"myInt" is required`. If the `"myString"` field was supplied with the value `"foo"`, it would throw a `BsonParseException` with the message `"myString" must begin with "the prefix"`.

Higher level code that has the actual `BsonDocument` that it wishes to deserialize into a `Foo` should do so by creating a `DocumentParser` via `BsonDocumentParser.create(doc)`:

```java
public Foo getFoo() throws BsonParseException {
  BsonDocument doc = getDoc();

  try (var parser = BsonDocumentParser.create(doc)) {
    return Foo.fromBson(parser);
  }
}
```

Note that `DocumentParser` is `AutoClosable`. When `DocumentParser::close` is run it will look in the wrapped `BsonDocument` to see if there are any fields that exist that were not specified via a `DocumentParser::getField` call, and throw a `BsonParseException` if so.

For example, if we supplied the following BSON:

```json5
{
  myInt: 5,
  myString: "the prefix is good",
  unknownField: true,
  otherUnknownField: 13,
}
```

`DocumentParser::close` would throw a `BsonParseException` with the message `unrecognized fields ["unknownField", "otherUnknownField"]`.

### Field Groups

Sometimes groups of fields have semantic requirements, such as being mutually exclusive. In order to validate this you can use a `ParsedFieldGroup`, supplied by `DocumentParser::getGroup`.

Consider a case where there are 3 fields, each of type `Field.Optional<String>`, `FIELD_1`, `FIELD_2`, and `FIELD_3`.

#### Exactly One Field Present
If there should only ever be one of the fields supplied, but one of them must be supplied, call `ParsedFieldGroup::exacltyOneOf`. It will validate that only one of the fields was present, and return that value:

```java
// Note we do not call unwrap() on the returned values, 
// as we need the ParsedField.Optional<String> to pass to exactlyOneOf().
var field1 = parser.getField(FIELD_1);
var field2 = parser.getField(FIELD_2);
var field3 = parser.getField(FIELD_3);
String value = parser.getGroup().exactlyOneOf(field1, field2, field3);
```

`ParsedFieldGroup::exactlyOneOf` will throw a `BsonParseException` if none of the fields were present, with the message `one of ["field1", "field2", "field3"] must be present`.

If multiple fields were present it will throw a `BsonParseException` with the message `only one of ["field1", "field2", "field3"] may be present`.

#### At Most One Field Present

Similar to `exactlyOneOf()`, `ParsedFieldGroup` has `atMostOneOf()` if you don't require any of the fields be present. It returns `Optional<T>` instead of `T`:

```java
Optional<String> maybeValue = parser.getGroup().atMostOneOf(field1, field2, field3);
```

#### At Least One Field Present

If you just want to validate that at least one of the values is present, call `ParsedFieldGroup::atLeastOneOf`. It will throw a `BsonParseException` if none of the fields are present.

```java
parser.getGroup().atLeastOneOf(field1, field2, field3);
```

### Lists

List fields can be specified using the same builders. The elements of the list are specified by creating a builder from `Value` instead of `Field`, which does not require a field name but is otherwise the same:

```java
Field.Required<List<Integer>> MY_LIST =
    Field.builder("myList")
        .listOf(
            Value.builder()
                .intValue()
                /*
                 * The element builder can specify validation
                 * which will be applied to each element.
                 */
                .mustBePositive()
                .required())
        /* Validations are available on the list itself as well. */
        .mustNotBeEmpty()
        .required();
```

A `BsonParseException` will be passed with an informative message if one of the elements fails validation.

For example, if we had

```json5
{ myList: [5, -5] }
```

a `BsonParseException` would be thrown with the message `myList[1] must be positive`.

Sometimes you want to allow a single value of the element type to be allowed as well as a list. For example:

```json5
{ myList: 5 }
``` 

can be treated the same as

```json5
{ myList: [5] }
```
To allow for this call `singleValueOrListOf` instead of `listOf`:

```java
Field.Required<List<Integer>> MY_LIST =
    Field.builder("myList")
        .singleValueOrListOf(
            Value.builder()
                .intValue()
                .mustBePositive()
                .required())
        .mustNotBeEmpty()
        .required();
```

In simple, common cases where you have a single dimensional list of required values, you can use the `asList()` or `asSingleValueOrList()` on a type builder to create a list:

```java
Field.Required<List<Integer>> MY_LIST =
    Field.builder("myList")
        .intField()
        .mustBePositive()
        .asList()
        .required();
```

### Nested Classes

You often have a tree of nested BSON objects rather than just a single, flat BSON object. You'll want to parse these nested BSON objects into Java classes. If the Java class that you want to parse a nested object into has the static factory method mentioned above, this can be seamless.

Imagine we had an object that contained a `Foo` object under the field`"foo"`:

```json5
{
  myFoo: {
    myInt: 5,
    myString: "the prefix is good",
  },
  myOuterString: "hello",
}
```

We can model this outer object with the `OuterType` class, and create the nested `Foo` object with a `classField()` builder, using `Foo`'s static factory method:

```java
public class OuterType {

  public static class Fields {

    /* Type specific builders will offer some built in validation options. */
    public static final Field.Required<Foo> MY_TYPE =
        Field.builder("myFoo")
            /* Specify the static factory method. */
            .classField(Foo::fromBson)
            /* Supports arbitrary validation. */
            .validate(
                foo ->
                    foo.getMyInt() == 5
                        ? Optional.of("\"myInt\" cannot be 5")
                        : Optional.empty())
            .required();

    /* All builders also offer generic validation. */
    public static final Field.Optional<String> MY_OUTER_STRING =
        Field.builder("myOuterString")
            .stringField()
            .mustNotBeEmpty()
            .optional()
            .noDefault();
  }

  private OuterType(Foo foo, Optional<String> myString) {
    ...
  }

  public static OuterType fromBson(DocumentParser parser) throws BsonParseException {
    var myFoo = parser.getField(Fields.MY_TYPE).unwrap();
    var myOuterString = parser.getField(Fields.MY_OUTER_STRING).unwrap();

    return new OuterType(myFoo, myOuterString);
  }
 
  ...
}
```


The BSON Parser framework will handle propagating the correct context and creating relevant error messages. For example, imagine `OuterType` also had a nested optional `OuterType` field called `"myOuterType"`, and we parsed the following BSON as an `OuterType`:

```json5
{
  myOuterType: {
    myFoo: {
      myInt: -5,
      myString: "the prefix is good",
    }
  }
}
```

Recall that `Foo`'s `"myInt"` must be positive. Parsing this BSON would throw a `BsonParseException` with the message `"myInt" must be positive (from "myOuterType.myFoo")`.

#### Deserializing Raw BsonValues
Sometimes you need even more fine-grained control over how a nested class is deserialized, such as accepting a number of different BSON types. This can be accomplished by supplying the `classField()` builder with a method that accepts the `BsonParseContext` and raw `BsonValue` to deserialize the value as you wish, returning a `T`.

For example, imagine you wanted to deserialize a `boolean` that also accepted the strings `"true"` and `"false"`:

```java
public static final Field.Required<Boolean> MY_BOOL =
    Field.builder("myBool")
        .classField(
            /* In real code, this should be factored out somewhere. */
            (BsonParseContext context, BsonValue value) -> {
              switch (value.getBsonType()) {
                case BOOLEAN:
                  return value.asBoolean().getValue();

                case STRING:
                  switch (value.asString().getValue()) {
                    case "true":
                      return true;

                    case "false":
                      return false;

                    default:
                      context.handleSemanticError("must be \"true\" or \"false\"");
                      return Check.unreachable();
                  }

                default:
                  context.handleUnexpectedType(TypeDescription.BOOLEAN, value.getBsonType());
                  return Check.unreachable();
              }
            })
        .required();
```

### Maps

Occasionally you have a nested object where you know what the values' type is, but the keys in the map can be dynamic. This can be accomplished using the `mapOf()` builder:

```java
public static final Field.Required<Map<String, Integer>> COUNTS =
    Field.builder("counts")
        .mapOf(
            Value.builder()
                .intValue()
                .mustBeNonNegative()
                .required())
        .validateKeys(
            s -> s.contains(".") ? Optional.of("cannot contain \".\"") : Optional.empty())
        .required();
```

Similar to `List` fields, for simple cases you can use the `asMap` helper on type builders:

```java
Field.Required<Map<String, Integer>> MY_LIST =
    Field.builder("myList")
        .intField()
        .mustBePositive()
        .asMap()
        .required();
```


### Enums

Enums may be parsed from strings.

```java

  private enum MyEnum {
    FIRST_VALUE,
    SECOND_VALUE,
  }

  static Field.Required<MyEnum> MY_ENUM =
      Field.builder("myEnum")
          .enumField(MyEnum.class)
          .asCamelCase() 
          .required();

```

Using `.asCamelCase()` makes this field accept strings only as 
lower camel case. E.g: `firstValue` or `secondValue`.  


### Encoding

In addition to parsing BSON values into Java objects, the library allows for easy serializing of Java objects back into BSON. Classes can easily convert themselves to `BsonDocuments` using a `BsonDocumentBuilder`:

Using the `Foo` class described above, you could simply write the following:

```java
public class Foo {

  ...

  private Foo(int myInt, String myString, Optional<String> myOtherString, boolean myBool) {
    ...
  }

  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MY_INT, this.myInt)
        .field(Fields.MY_STRING, this.myString)
        .field(Fields.MY_OTHER_STRING, this.myOtherString)
        .field(Fields.MY_BOOL, this.myBool)
        .build();
  }

  ...
}
```

When creating a `Field` on a class, you must provide a method to encode the class. If the class implements [`Encodable`](Encodable.java), this can be automatically inferred:

```java
public class Bar {
  
  public static class Fields {

    /* Classes need to specify how to encode them to bson. */
    public static final Field.Required<Buzz> MY_BUZZ =
        Field.builder("buzz")
            .classField(Buzz::fromBson, OtherBuzzEncodingClass::encodeBuzz)
            .required();

    /* Classes that implement Encodable do not need to specify toBson as their encoding method. */
    public static final Field.Required<Qux> MY_QUX =
        Field.builder("qux")
            .classField(Qux::fromBson) 
            .required();
  }

  public static class Buzz {
    ...
  }

  public static class Qux implements Encodable {
    ...
  }

  ...
}
```

Note that this means that every class that is included in a `Field` must have a way to encode it, even if you don't plan on encoding it. Hopefully the ease of the `BsonDocumentBuilder` API makes this more tractable.

## Implementation

### ValueParser

At runtime, the goal of the `DocumentParser` is to transform a `BsonValue` into a Java object of some type `T`, and create a rich exception based on the context in the BSON tree if the BSON does not match the expected schema for any reason.

To model this, we have the [`ValueParser<T>`](ValueParser.java) interface.

The goal of the different builder classes is ultimately to build a `ValueParser<T>` that returns a valid value of type `T` after validating that it is semantically valid. To this effect, the `<type>Field()` builder method internally produces a `ValueParser<type>`, which is then wrapped with higher level `ValueParsers` that validate other semantics prior to calling this base `ValueParser`, or validate the semantics of the value returned by the base `ValueParser`.

For example:

```java
Field.Optional<Integer> MY_INT =
  Field.builder("myInt")
      .intField()       /* Creates a ValueParser<Integer>. */
      .mustBePositive() /* Creates a ValueParser<Integer> that invokes the
                           above and checks that the value produced is positive. */
      .optional()
      .noDefault()      /* Creates a ValueParser<Optional<Integer>> that first
                           checks if the BsonValue passed in is null prior to
                           invoking the ValueParser<Integer> created by 
                           mustBePositive(). */;
```

Put differently, this will create the following chain of `ValueParser`s:

```
1. ValueParser<Optional<Integer>> // Invokes 2 if value is not null, otherwise returns empty().
2.  ValueParser<Integer>          // Invokes 3, then checks value is positive.
3.   ValueParser<Integer>         // Parses an Integer from the BsonValue.
```

### Type Safety

The builder APIs are designed to enforce that users must supply all required fields, and to check this with the type system rather than at runtime.

As such, there is no single `Builder` class that contains all of the options, with a `build()` method to return a `Field`. Instead, there are different builder classes that are returned for each stage (i.e. required option) of the builder:

```java
Field.Optional<Integer> MY_INT =
  Field.builder("myInt") /* Returns a Field.TypeBuilder, which requires you choose a field type. */
      .intField()        /* Returns a IntegerField.FieldBuilder which allows you to
                            specify validations or move on to the optional/required stage. */
      .mustBePositive()  /* Returns a IntegerField.FieldBuilder, same as above. */
      .optional()        /* Returns a OptionalField.FieldBuilder<Integer>, which requires you
                            to specify if there is a default or not. */
      .noDefault()       /* Returns the actual Field.Optional<Integer>. */;
```
