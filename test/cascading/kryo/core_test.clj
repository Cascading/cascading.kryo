(ns cascading.kryo.core-test
  (:use clojure.test)
  (:import org.apache.hadoop.mapred.JobConf
           com.esotericsoftware.kryo.Kryo
           [cascading.kryo KryoFactory KryoSerialization
            KryoSerializer KryoDeserializer]))

(defn get-serializations [factory conf]
  (vec (map vec (.getSerializations factory conf))))

(deftest invalid-serializations-test
  (testing "Anything other than a sequence of arrays with one or two
  entries should throw an exception."
      (are [pairs]
           (thrown? RuntimeException
                    (.setSerializations (KryoFactory.) (JobConf.) pairs))
           [["class" "serial" "face"] ["k" "v"]]
           [[] ["k" "v"]])))

(deftest valid-serializations-test
  (testing  "sequences of either individual classes or class-serializer
  pairs should round trip."
    (let [conf (JobConf.)
          factory (KryoFactory.)]
      (are [pairs]
           (do (.setSerializations factory conf pairs)
               (= pairs (get-serializations factory conf)))
           [["class" "serial"] ["k" "v"]]
           [["k"] ["v"] ["x"]]))))

(deftest string-builder-test
  (let [conf (JobConf.)
        factory (KryoFactory.)]
    (are [pairs string]
         (do (.setSerializations factory conf pairs)
             (= string (.get conf KryoFactory/KRYO_SERIALIZATIONS)))
         [["k" "v"]] "k,v"
         [["k"]] "k"
         [["k" "v"] ["i" "j"]] "k,v:i,j")))

(deftest from-string-test
  (let [conf (JobConf.)
        factory (KryoFactory.)]
    (are [string pairs]
         (do (.set conf KryoFactory/KRYO_SERIALIZATIONS string)
             (= pairs (get-serializations factory conf)))
         "k,v" [["k" "v"]]
         "k" [["k"]] 
         "k,v:i,j" [["k" "v"] ["i" "j"]])))

(defn klass [class-name]
  (Class/forName class-name))

(defn same-size? [& xs]
  (reduce = (map count xs)))

(deftest fill-kryo-test
  (are [name-seq]
       (let [kryo (Kryo.)]
         (do (KryoFactory/populateKryo kryo (map vector name-seq) false false)
             (same-size? name-seq
                         (for [name name-seq :let [k (klass name)]]
                           (.getRegisteredClass kryo k)))))
       ["java.util.HashMap" "java.util.HashSet"]))
