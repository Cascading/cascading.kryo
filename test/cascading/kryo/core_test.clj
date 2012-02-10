(ns cascading.kryo.core-test
  (:use midje.sweet)
  (:import [org.apache.hadoop.mapred JobConf]
           [cascading.kryo Kryo KryoFactory KryoSerialization]
           [java.io ByteArrayOutputStream ByteArrayInputStream]))

(defn os->is
  "Returns a ByteArrayInputStream into the data inside the supplied
  ByteArrayOutputStream."
  [os]
  (ByteArrayInputStream. (.toByteArray os)))

(defn round-trip
  "Accepts a serialization and an object, round trips the object
  through the Hadoop serialization and passes it back out. Successful
  round-tripping will act as identity."
  [serialization obj]
  (let [klass   (class obj)
        os      (ByteArrayOutputStream.)]
    (with-open [freezer (doto (.getSerializer serialization klass)
                          (.open os)
                          (.serialize obj))
                thawer (doto (.getDeserializer serialization klass)
                         (.open (os->is os)))]
      (.deserialize thawer obj))))

(defmacro truthy-doto
  "Evaluates x then calls all of the methods and functions with the
  value of x supplied at the front of the given arguments.  The forms
  are evaluated in order.  Returns x.

  (doto (new java.util.HashMap) (.put \"a\" 1) (.put \"b\" 2))"
  {:added "1.0"}
  [x & forms]
  (let [gx (gensym)]
    `(let [~gx ~x]
       ~@(map (fn [f]
                (if (seq? f)
                  `(when (seq (keep identity [~@(next f)]))
                     (~(first f) ~gx ~@(next f)))
                  `(~f ~gx)))
              forms)
       ~gx)))

(defn factory
  [& {:keys [conf accept-all skip-missing
             serializations hierarchies]
      :or {conf (JobConf.)}}]
  (truthy-doto (KryoFactory. conf)
               (.setAcceptAll accept-all)
               (.setSkipMissing skip-missing)
               (.setHierarchyRegistrations hierarchies)
               (.setSerializations serializations)))

(tabular
 (fact "Anything other than a sequence of Lists with one or two
  entries should throw an exception."
   (factory :serializations ?pairs) => (throws RuntimeException))
 ?pairs
 [["class" "serial" "face"] ["k" "v"]]
 [[] ["k" "v"]])

(defn get-serializations
  "Returns a clojure representation of the serializations registered
  by the supplied factory."
  [factory]
  (into [] (map vec (.getSerializations factory))))

(tabular
 (fact "sequences of either individual classes or class-serializer
  pairs should round trip through a KryoFactory."
   (let [factory (factory :serializations ?pairs)]
     (get-serializations factory) => ?pairs))
 ?pairs
 [["class" "serial"] ["k" "v"]]
 [["k"] ["v"] ["x"]])

(tabular
 (fact "Pairs of klass & serialization should be resolved into a
       list of lists by KryoFactory."
   (let [factory (factory
                  :conf (doto (JobConf.)
                          (.set KryoFactory/KRYO_SERIALIZATIONS ?string)))]
     (get-serializations factory) => ?pairs))
 ?string   ?pairs
 "k,v"     [["k" "v"]]
 "k"       [["k"]] 
 "k,v:i,j" [["k" "v"] ["i" "j"]])

(defn klass [class-name]
  (Class/forName class-name))

(defn round-trip [reg-map]
  (let [kryo     (Kryo.)
        klasses  (keys reg-map)
        factory  (doto (factory :serializations (map vector klasses)
                                :hierarchies    (into [] reg-map))
                   (.populateKryo kryo))]    
    (into {} (for [klass-name klasses
                   :let [reg  (.getRegisteredClass kryo (klass klass-name))
                         type        (.getType reg)
                         ser  (class (.getSerializer reg))]]
               [(.getName type) (.getName ser)]))))

(let [hierarchies {"java.util.HashMap"
                   "com.esotericsoftware.kryo.serialize.ByteSerializer"
                   "java.util.HashSet"
                   "com.esotericsoftware.kryo.serialize.ClassSerializer"}]
  (fact (round-trip hierarchies) => hierarchies))
