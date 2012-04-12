(defproject cascading.kryo "0.3.0"
  :description "Kryo serialization for Cascading."
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo/"}
  :dependencies [[com.esotericsoftware.kryo/kryo "2.02"]
                 [de.javakaffee/kryo-serializers "0.20"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje "1.3.1" :exclusions [org.clojure/clojure]]
                     [clojure "1.2.1"]
                     [lein-midje "1.0.8"]])
