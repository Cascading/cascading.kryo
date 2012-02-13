(ns cascading.kryo.core-test
  (:use midje.sweet)
  (:import [org.apache.hadoop.mapred JobConf]
           [cascading.kryo Kryo KryoFactory KryoSerialization
            KryoFactory$ClassPair]
           [java.io ByteArrayOutputStream ByteArrayInputStream]
           [com.esotericsoftware.kryo.serialize LongSerializer]))

(defn pair
  ([klass-a]
     (KryoFactory$ClassPair. klass-a))
  ([klass-a klass-b]
     (KryoFactory$ClassPair. klass-a klass-b)))

(defn to-pairs [pair-seq]
  (when pair-seq
    (map (partial apply pair) pair-seq)))

(defn os->is
  "Returns a ByteArrayInputStream into the data inside the supplied
  ByteArrayOutputStream."
  [os]
  (ByteArrayInputStream. (.toByteArray os)))

(defn round-trip-obj
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
             registrations hierarchies]
      :or {conf (JobConf.)}}]
  (truthy-doto (KryoFactory. conf)
               (.setAcceptAll accept-all)
               (.setSkipMissing skip-missing)
               (.setHierarchyRegistrations (to-pairs hierarchies))
               (.setRegistrations (to-pairs registrations))))

(defn get-registrations
  "Returns a clojure representation of the serializations registered
  by the supplied factory."
  [factory]
  (map (fn [pair]
         (let [super-klass  (.getSuperClass pair)]
           (if-let [serial-klass (.getSerializerClass pair)]
             [super-klass serial-klass]
             [super-klass])))
       (.getRegistrations factory)))

(tabular
 (fact "sequences of either individual classes or class-serializer
  pairs should round trip through a KryoFactory."
   (let [factory (factory :registrations ?pairs)]
     (get-registrations factory) => ?pairs))
 ?pairs
 [[String LongSerializer]])

(tabular
 (fact "Pairs of klass & serialization should be resolved into a
       list of lists by KryoFactory."
   (let [factory (factory
                  :conf (doto (JobConf.)
                          (.set KryoFactory/KRYO_REGISTRATIONS ?string)))]
     (get-registrations factory) => ?pairs))
 ?string                             ?pairs
 "java.lang.String,java.lang.String" [[String String]])

(defn klass [class-name]
  (Class/forName class-name))

(defn round-trip [reg-map]
  (let [kryo     (Kryo.)
        klasses  (keys reg-map)
        factory  (doto (factory :registrations (map vector klasses)
                                :hierarchies   (into [] reg-map))
                   (.populateKryo kryo))]    
    (into {} (for [klass-name klasses
                   :let [reg  (.getRegisteredClass kryo klass-name)]]
               [(.getType reg)
                (class (.getSerializer reg))]))))
