(defproject cascading.kryo "0.1.4"
  :description "Kryo serialization for Cascading."
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo"}
  :dependencies [[cascading/cascading-core "1.2.4"
                  :exclusions [org.codehaus.janino/janino]]
                 [com.googlecode/kryo "1.04"]
                 [clojure "1.3.0"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]])
