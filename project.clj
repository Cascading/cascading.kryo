(defproject cascading.kryo "0.1.7"
  :description "Kryo serialization for Cascading."
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[com.googlecode/kryo "1.04"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [clojure "1.3.0"]])
