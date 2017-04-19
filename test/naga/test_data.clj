(ns naga.test-data
  (:require [clojure.test :refer :all]
            [naga.data :refer :all]
            [naga.storage.test :as st]
            [naga.store :as store]
            [naga.storage.memory.core :refer [empty-store]]))

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
