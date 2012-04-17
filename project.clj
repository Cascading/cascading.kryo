(defproject cascading.kryo "0.3.1"
  :description "Kryo serialization for Cascading."
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo/"}
  :dependencies [[com.twitter/kryo "2.04"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje "1.3.1" :exclusions [org.clojure/clojure]]
                     [clojure "1.2.1"]
                     [lein-midje "1.0.8"]])
