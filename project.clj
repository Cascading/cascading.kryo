(defproject cascading.kryo "0.4.6"
  :description "Kryo serialization for Cascading."
  :url "https://github.com/Cascading/cascading.kryo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :plugins [[lein-clojars "0.9.1"]]
  :repositories {"conjars" "http://conjars.org/repo"}
  :dependencies [[com.esotericsoftware.kryo/kryo "2.17"]]
  :profiles {:dev
             {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                             [midje "1.4.0" :exclusions [org.clojure/clojure]]
                             [org.clojure/clojure "1.4.0"]
                             [lein-midje "1.0.9"]]}})
