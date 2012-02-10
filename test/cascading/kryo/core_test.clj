(ns cascading.kryo.core-test
  (:use clojure.test)
  (:import org.apache.hadoop.mapred.JobConf
           cascading.kryo.Kryo
           [cascading.kryo KryoFactory KryoSerialization
            KryoSerializer KryoDeserializer]))

(defn get-serializations [factory]
  (vec (map vec (.getSerializations factory))))

(deftest invalid-serializations-test
  (testing "Anything other than a sequence of arrays with one or two
  entries should throw an exception."
    (are [pairs]
         (thrown? RuntimeException
                  (.setSerializations (KryoFactory. (JobConf.)) pairs))
         [["class" "serial" "face"] ["k" "v"]]
         [[] ["k" "v"]])))

(deftest valid-serializations-test
  (testing  "sequences of either individual classes or class-serializer
  pairs should round trip."
    (let [factory (KryoFactory. (JobConf.))]
      (are [pairs]
           (do (.setSerializations factory pairs)
               (= pairs (get-serializations factory)))
           [["class" "serial"] ["k" "v"]]
           [["k"] ["v"] ["x"]]))))

(deftest string-builder-test
  (let [factory (KryoFactory. (JobConf.))]
    (are [pairs]
         (do (.setSerializations factory pairs)
             (= pairs (.getSerializations factory)))
         [["k" "v"]]
         [["k"]]
         [["k" "v"] ["i" "j"]])))

(deftest from-string-test
  (are [string pairs]
       (let [conf    (doto (JobConf.)
                       (.set KryoFactory/KRYO_SERIALIZATIONS string))
             factory (KryoFactory. conf)]
         (= pairs (get-serializations factory))
         "k,v"     [["k" "v"]]
         "k"       [["k"]] 
         "k,v:i,j" [["k" "v"] ["i" "j"]])))

(defn klass [class-name]
  (Class/forName class-name))

(defn same-size? [& xs]
  (reduce = (map count xs)))

(deftest fill-kryo-test
  (are [name-seq]
       (let [kryo    (Kryo.)
             factory (doto (KryoFactory. (JobConf.))
                       (.setSerializations (map vector name-seq))
                       (.setAcceptAll false)
                       (.setSkipMissing false))]
         (do (.populateKryo factory kryo)
             (same-size? name-seq
                         (for [name name-seq :let [k (klass name)]]
                           (.getRegisteredClass kryo k)))))
       ["java.util.HashMap" "java.util.HashSet"]))
