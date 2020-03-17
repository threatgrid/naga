(ns naga.test-data
  (:require [naga.data :refer [string->triples json->triples store->json json-update->triples ident-map->triples]]
            [naga.storage.test-helper :as test-helper]
            [naga.store :as store :refer [query assert-data retract-data]]
            [asami.core :refer [empty-store]]
            [asami.multi-graph]
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]])
            #?(:clj  [clojure.test :as t :refer [is]]
               :cljs [clojure.test :as t :refer-macros [is]]))
  #?(:clj (:import [java.time ZonedDateTime])))

(t/use-fixtures :once st/validate-schemas)

(deftest test-encode-from-string
  (let [m1 (string->triples (test-helper/new-store)
                            "[{\"prop\": \"val\"}]")
        m2 (string->triples (test-helper/new-store)
                            "[{\"prop\": \"val\", \"p2\": 2}]")
        m3 (string->triples (test-helper/new-store)
                            (str "[{\"prop\": \"val\","
                                 "  \"p2\": 22,"
                                 "  \"p3\": [42, 54]}]"))
        m4 (string->triples (test-helper/new-store)
                            (str "[{\"prop\": \"val\"},"
                                 " {\"prop\": \"val2\"}]"))
        m5 (string->triples (test-helper/new-store)
                            (str "[{\"prop\": \"val\","
                                 "  \"arr\": ["
                                 "    {\"a\": 1},"
                                 "    {\"a\": 2},"
                                 "    [\"nested\"]"
                                 "]}]"))]
    (is (= [[:test/n1 :db/ident :test/n1]
            [:test/n1 :naga/entity true]
            [:test/n1 :prop "val"]] m1))
    (is (= [[:test/n1 :db/ident :test/n1]
            [:test/n1 :naga/entity true]
            [:test/n1 :prop "val"]
            [:test/n1 :p2 2]] m2))
    (is (= [[:test/n1 :db/ident :test/n1] 
            [:test/n1 :naga/entity true]
            [:test/n1 :prop "val"]
            [:test/n1 :p2 22]
            [:test/n1 :p3 :test/n2]
            [:test/n2 :naga/first 42]
            [:test/n2 :naga/rest :test/n3]
            [:test/n3 :naga/first 54]
            [:test/n2 :naga/contains 42]
            [:test/n2 :naga/contains 54]] m3))
    (is (= [[:test/n1 :db/ident :test/n1]
            [:test/n1 :naga/entity true]
            [:test/n1 :prop "val"]
            [:test/n2 :db/ident :test/n2]
            [:test/n2 :naga/entity true]
            [:test/n2 :prop "val2"]] m4))
    (is (= [[:test/n1 :db/ident :test/n1]
            [:test/n1 :naga/entity true]
            [:test/n1 :prop "val"]
            [:test/n1 :arr :test/n2]
            [:test/n2 :naga/first :test/n3]
            [:test/n2 :naga/rest :test/n4]
            [:test/n3 :a 1]
            [:test/n4 :naga/first :test/n5]
            [:test/n4 :naga/rest :test/n6]
            [:test/n5 :a 2]
            [:test/n6 :naga/first :test/n7]
            [:test/n7 :naga/first "nested"]
            [:test/n7 :naga/contains "nested"]
            [:test/n2 :naga/contains :test/n3]
            [:test/n2 :naga/contains :test/n5]
            [:test/n2 :naga/contains :test/n7]] m5))))

(deftest test-encode
  (let [m1 (json->triples (test-helper/new-store)
                          [{:prop "val"}])
        m2 (json->triples (test-helper/new-store)
                          [{:prop "val", :p2 2}])
        m3 (json->triples (test-helper/new-store)
                          [{:prop "val", :p2 22, :p3 [42 54]}])
        m4 (json->triples (test-helper/new-store)
                          [{:prop "val"} {:prop "val2"}])
        m5 (json->triples (test-helper/new-store)
                          [{:prop "val"
                            :arr [{:a 1} {:a 2} ["nested"]]}])]
    (is (= [[:test/n1 :db/ident :test/n1]
            [:test/n1 :naga/entity true]
            [:test/n1 :prop "val"]] m1))
    (is (= [[:test/n1 :db/ident :test/n1]
            [:test/n1 :naga/entity true]
            [:test/n1 :prop "val"]
            [:test/n1 :p2 2]] m2))
    (is (= [[:test/n1 :db/ident :test/n1] 
            [:test/n1 :naga/entity true]
            [:test/n1 :prop "val"]
            [:test/n1 :p2 22]
            [:test/n1 :p3 :test/n2]
            [:test/n2 :naga/first 42]
            [:test/n2 :naga/rest :test/n3]
            [:test/n3 :naga/first 54]
            [:test/n2 :naga/contains 42]
            [:test/n2 :naga/contains 54]] m3))
    (is (= [[:test/n1 :db/ident :test/n1] 
            [:test/n1 :naga/entity true]
            [:test/n1 :prop "val"]
            [:test/n2 :db/ident :test/n2] 
            [:test/n2 :naga/entity true]
            [:test/n2 :prop "val2"]] m4))
    (is (= [[:test/n1 :db/ident :test/n1] 
            [:test/n1 :naga/entity true]
            [:test/n1 :prop "val"]
            [:test/n1 :arr :test/n2]
            [:test/n2 :naga/first :test/n3]
            [:test/n2 :naga/rest :test/n4]
            [:test/n3 :a 1]
            [:test/n4 :naga/first :test/n5]
            [:test/n4 :naga/rest :test/n6]
            [:test/n5 :a 2]
            [:test/n6 :naga/first :test/n7]
            [:test/n7 :naga/first "nested"]
            [:test/n7 :naga/contains "nested"]
            [:test/n2 :naga/contains :test/n3]
            [:test/n2 :naga/contains :test/n5]
            [:test/n2 :naga/contains :test/n7]] m5))))

(defn round-trip
  [data]
  (let [m (json->triples empty-store data)
        new-db (store/assert-data empty-store m)]
    (set (store->json new-db))))

(deftest test-round-trip
  (let [d1 #{{:prop "val"}}
        dr1 (round-trip d1)

        d2 #{{:prop "val", :p2 2}}
        dr2 (round-trip d2)

        d3 #{{:prop "val", :p2 22, :p3 [42 54]}}
        dr3 (round-trip d3)

        d4 #{{:prop "val"} {:prop "val2"}}
        dr4 (round-trip d4)

        d5 #{{:prop "val" :arr [{:a 1} {:a 2} ["nested"]]}}
        dr5 (round-trip d5)

        d6 #{{:prop "val" :arr [{:a 1} {:a 2} ["nested"]] :nested {}}}
        dr6 (round-trip d6)

        d7 #{{:prop "val" :arr (map identity [{:a 1} {:a 2} ["nested"]]) :nested {}}}
        dr7 (round-trip d7)]
    (is (= d1 dr1))
    (is (= d2 dr2))
    (is (= d3 dr3))
    (is (= d4 dr4))
    (is (= d5 dr5))
    (is (= d6 dr6))
    (is (= d7 dr7))))

(defn generate-diff
  [o1 o2]
  (let [triples (json->triples empty-store [o1])
        props (filter (fn [[k v]] (or (number? v) (string? v))) o1)
        st (store/assert-data empty-store triples)
        id (ffirst (store/query st '[?id] (map (fn [[k v]] ['?id k v]) props)))
        [additions retractions] (json-update->triples st id o2)]
    [id additions retractions]))

(deftest test-updates
  (let [[id1 add1 ret1] (generate-diff {:a 1} {:a 2})
        [id2 add2 ret2] (generate-diff {:a 1 :b "foo"} {:a 2 :b "foo"})
        [id3 add3 ret3] (generate-diff {:a 1 :b "foo"} {:a 1 :b "bar"})
        [id4 add4 ret4] (generate-diff {:a 1 :b "foo" :c [10 11 12] :d {:x "a" :y "b"} :e [{:m 1} {:m 2}]}
                                       {:b "bar", :c [10 10 12], :e [{:m 1} {:m 2}], :bx "xxx"})]
    (is (= add1 [[id1 :a 2]]))
    (is (= ret1 [[id1 :a 1]]))
    (is (= add2 [[id2 :a 2]]))
    (is (= ret2 [[id2 :a 1]]))
    (is (= add3 [[id3 :b "bar"]]))
    (is (= ret3 [[id3 :b "foo"]]))
    (let [adds (filter #(#{:b :c :bx} (second %)) add4)
          dels (filter #(#{:a :b :c :d} (second %)) ret4)
          [[sid1 _ a1] [sid2 _ a2] :as ds] (filter (fn [[_ p _]] (#{:x :y} p)) ret4)]
      (is (= 3 (count adds)))
      (is (= 4 (count dels)))
      (is (= sid1 sid2))
      (is (= #{"a" "b"} #{a1 a2})))))

(defn get-node-ref
  [store id]
  (ffirst (query store '[?n] [['?n :id id]])))


#?(:clj
(deftest test-multi-update
  (let [graph
        #asami.multi_graph.MultiGraph{:spo #:mem{:node-27367
                                                 {:db/ident #:mem{:node-27367 1},
                                                  :naga/entity {true 1},
                                                  :value {"01468b1d3e089985a4ed255b6594d24863cfd94a647329c631e4f4e52759f8a9" 1},
                                                  :type {"sha256" 1},
                                                  :id {"4f390192" 1}}},
                                      :pos {:db/ident
                                            #:mem{:node-27367 #:mem{:node-27367 1}},
                                            :naga/entity {true #:mem{:node-27367 1}},
                                            :value {"01468b1d3e089985a4ed255b6594d24863cfd94a647329c631e4f4e52759f8a9" #:mem{:node-27367 1}},
                                            :type {"sha256" #:mem{:node-27367 1}},
                                            :id {"4f390192" #:mem{:node-27367 1}}},
                                      :osp {:mem/node-27367 #:mem{:node-27367 #:db{:ident 1}},
                                            true #:mem{:node-27367 #:naga{:entity 1}},
                                            "01468b1d3e089985a4ed255b6594d24863cfd94a647329c631e4f4e52759f8a9"
                                            #:mem{:node-27367 {:value 1}},
                                            "sha256" #:mem{:node-27367 {:type 1}},
                                            "4f390192" #:mem{:node-27367 {:id 1}}}}
        store (asami.core.MemoryStore. nil graph)
        id "verdict:AMP File Reputation:4f390192"
        m {:type "verdict",
           :disposition 2,
           :observable {:value "01468b1d3e089985a4ed255b6594d24863cfd94a647329c631e4f4e52759f8a9",
                        :type "sha256"},
           :disposition_name "Malicious",
           :valid_time {:start_time (ZonedDateTime/parse "2017-12-05T12:45:32.192Z"),
                        :end_time (ZonedDateTime/parse "2525-01-01T00:00Z")},
           :module-name "AMP File Reputation",
           :id "verdict:AMP File Reputation:4f390192"}
        new-store (if-let [n (get-node-ref store id)]
                    (let [[assertions retractions] (json-update->triples store n m)
                          assert-keys (set (map (fn [[a b c]] [a b]) assertions))
                          retract-existing (filter (fn [[a b c]] (assert-keys [a b])) retractions)]
                      (-> store
                          (retract-data retract-existing)
                          (assert-data assertions)))
                    (let [assertions (ident-map->triples store (assoc m :id id))]
                      (assert-data store assertions)))]
    (is (= 4 (count (:spo (:graph new-store)))))))
)

#?(:cljs (t/run-tests))
