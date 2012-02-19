(defproject cascading.kryo "0.2.0"
  :description "Kryo serialization for Cascading."
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[com.googlecode/kryo "1.04"]
                 [de.javakaffee/kryo-serializers "0.9"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje "1.3.1" :exclusions [org.clojure/clojure]]
                     [clojure "1.2.1"]
                     [lein-midje "1.0.8"]])
