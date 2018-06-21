(ns naga.test-data
  (:require [naga.data :refer [string->triples json->triples store->json json-update->triples]]
            [naga.storage.test :as st]
            [naga.store :as store]
            [asami.core :refer [empty-store]]
            #?(:clj  [clojure.test :as t :refer [deftest is]]
               :cljs [clojure.test :as t :refer-macros [deftest is]])))

(deftest test-encode-from-string
  (let [m1 (string->triples (st/new-store)
                            "[{\"prop\": \"val\"}]")
        m2 (string->triples (st/new-store)
                            "[{\"prop\": \"val\", \"p2\": 2}]")
        m3 (string->triples (st/new-store)
                            (str "[{\"prop\": \"val\","
                                 "  \"p2\": 22,"
                                 "  \"p3\": [42, 54]}]"))
        m4 (string->triples (st/new-store)
                            (str "[{\"prop\": \"val\"},"
                                 " {\"prop\": \"val2\"}]"))
        m5 (string->triples (st/new-store)
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
  (let [m1 (json->triples (st/new-store)
                          [{:prop "val"}])
        m2 (json->triples (st/new-store)
                          [{:prop "val", :p2 2}])
        m3 (json->triples (st/new-store)
                          [{:prop "val", :p2 22, :p3 [42 54]}])
        m4 (json->triples (st/new-store)
                          [{:prop "val"} {:prop "val2"}])
        m5 (json->triples (st/new-store)
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
        dr5 (round-trip d5)]
    (is (= d1 dr1))
    (is (= d2 dr2))
    (is (= d3 dr3))
    (is (= d4 dr4))
    (is (= d5 dr5))))

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

#?(:cljs (t/run-tests))
