# cascading.kryo

Cascading.Kryo provides a drop-in serialization for using Kryo objects in your Hadoop workflow.

## Usage

Add the following entry to `"io.serializations` in your `JobConf`:

    cascading.kryo.KryoSerialization

## Configuring Kryo

Cascading.Kryo makes use of the following JobConf settings:

### cascading.kryo.skip.missing

### cascading.kryo.accept.all

### cascading.kryo.registrations

## License

Copyright (C) 2011 Sam Ritchie

Distributed under the Eclipse Public License, the same as Clojure.
