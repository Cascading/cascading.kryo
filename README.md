# cascading.kryo

Cascading.Kryo provides a drop-in [Kryo](http://code.google.com/p/kryo/) serialization for your Cascading (or Hadoop) workflow.

## Usage

Cascading.Kryo is hosted on www.clojars.org and www.conjars.org. See http://conjars.org/cascading.kryo for instructions on how to include Cascading.Kryo in your project.

Once that's done, activate the serialization by adding the following entry to `"io.serializations` in your `JobConf`:

    cascading.kryo.KryoSerialization

## Configuring Kryo

Cascading.Kryo makes use of the following JobConf entries:

### cascading.kryo.registrations

To provide custom registrations to Kryo, set `cascading.kryo.registrations` to a colon-separated string of <className, KryoSerializerName> pairs. For example:

    "someClass,someSerializer:otherClass:thirdClass,thirdSerializer"

would register `someClass` and `thirdClass` to use their respective custom serializers, and `otherClass` to use Kryo's `FieldSerializer`. The FieldSerializer requires the class to implement a default constructor. See the [Kryo page](http://code.google.com/p/kryo/) for more information.

`cascading.kryo.KryoFactory` provides the following helper methods:

```java
public void setRegistrations(List<ClassPair> registrations)
public List<ClassPair> getRegistrations()
```

### cascading.kryo.hierarchy.registrations

A Hierarchy registration is a pair of `<classOrInterface, serializerClass>`. Hierarchy pairs are examined in order after all explicit registrations, and catch objects that are assignable from the supplied `classOrInterface`. You can use this feature to catch all Lists, for example. Set `cascading.kryo.hierarchy.registrations` to a colon-separated string of <classOrInterface, serializerClass> pairs. For example:

    "someClass,someSerializer:someInterface,otherSerializer"

would register objects assignable from `someClass` and `someInterface` to be serialized by (respectively) `someSerializer` and `otherSerializer`.

`cascading.kryo.KryoFactory` provides the following helper methods:

```java
public void setHierarchyRegistrations(List<ClassPair> registrations)
public List<ClassPair> getHierarchyRegistrations()
```

### cascading.kryo.skip.missing

(Defaults to false.)

If this setting is false, and you provide a class or serializer in `cascading.kryo.registrations` that Kryo doesn't recognize, your job will throw an error. Set `cascading.kryo.skip.missing` to `true` to log the error and allow the job to proceed.

### cascading.kryo.accept.all

(Defaults to true.)

When `cascading.kryo.accept.all` is true, Cascading.Kryo will accept any class that comes its way in addition to explicitly-registered classes. This behavior makes Cascading.Kryo useful as a replacement for Java's default serialization. Set `cascading.kryo.accept.all` to false to have `KryoSerialization` reject un-registered classes.

## License

Distributed under the Eclipse Public License, the same as Clojure.
