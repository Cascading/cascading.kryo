(defproject cascading.kryo "0.4.1"
  :description "Kryo serialization for Cascading."
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[com.esotericsoftware.kryo/kryo "2.16"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje "1.4.0" :exclusions [org.clojure/clojure]]
                     [clojure "1.4.0"]
                     [lein-midje "1.0.9"]])
