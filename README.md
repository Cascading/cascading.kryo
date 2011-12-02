# cascading.kryo

Cascading.Kryo provides a drop-in [Kryo](http://code.google.com/p/kryo/) serialization for your Cascading (or Hadoop) workflow.

## Usage

Add the following entry to `"io.serializations` in your `JobConf`:

    cascading.kryo.KryoSerialization

## Configuring Kryo

Cascading.Kryo makes use of the following JobConf entries:

### cascading.kryo.registrations

To provide custom registrations to Kryo, set `cascading.kryo.registrations` to a colon-separated string of <className, KryoSerializerName> pairs. For example:

    "someClass,someSerializer:otherClass:thirdClass,thirdSerializer"

would register `someClass` and `thirdClass` to use their respective custom serializers, and `otherClass` to use Kryo's `FieldsSerializer`. The FieldsSerializer requires the class to implement a default constructor. See the [Kryo page](http://code.google.com/p/kryo/) for more information.

`cascading.kryo.KryoFactory` provides the following helper methods:

```java
public void setSerializations(JobConf conf, HashMap<String, String> registrations)
public HashMap getSerializations(JobConf conf)
```

### cascading.kryo.skip.missing

(Defaults to false.)

If this setting is false, and you provide a class or serializer in `cascading.kryo.registrations` that Kryo doesn't recognize, your job will throw an error. Set `cascading.kryo.skip.missing` to `true` to log the error and allow the job to proceed.

### cascading.kryo.accept.all

(Defaults to true.)

When `cascading.kryo.accept.all` is true, Cascading.Kryo will accept any class that comes its way in addition to explicitly-registered classes. This behavior makes Cascading.Kryo useful as a replacement for Java's loathesome default serialization. Set `cascading.kryo.accept.all` to false to have it reject un-registered classes.

## License

Distributed under the Eclipse Public License, the same as Clojure.
