(ns naga.test-data
  (:require [clojure.test :refer :all]
            [naga.data :refer :all]
            [naga.storage.test :as st]))

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
    (is (= [[:test/n1 :prop "val"]] m1))
    (is (= [[:test/n1 :prop "val"]
            [:test/n1 :p2 2]] m2))
    (is (= [[:test/n1 :prop "val"]
            [:test/n1 :p2 22]
            [:test/n1 :p3 :test/n2]
            [:test/n2 :naga/first 42]
            [:test/n2 :naga/rest :test/n3]
            [:test/n3 :naga/first 54]
            [:test/n3 :naga/rest :naga/nil]] m3))
    (is (= [[:test/n1 :prop "val"]
            [:test/n2 :prop "val2"]] m4))
    (is (= [[:test/n1 :prop "val"]
            [:test/n1 :arr :test/n2]
            [:test/n2 :naga/first :test/n3]
            [:test/n2 :naga/rest :test/n4]
            [:test/n3 :a 1]
            [:test/n4 :naga/first :test/n5]
            [:test/n4 :naga/rest :test/n6]
            [:test/n5 :a 2]
            [:test/n6 :naga/first :test/n7]
            [:test/n6 :naga/rest :naga/nil]
            [:test/n7 :naga/first "nested"]
            [:test/n7 :naga/rest :naga/nil]] m5))))

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
    (is (= [[:test/n1 :prop "val"]] m1))
    (is (= [[:test/n1 :prop "val"]
            [:test/n1 :p2 2]] m2))
    (is (= [[:test/n1 :prop "val"]
            [:test/n1 :p2 22]
            [:test/n1 :p3 :test/n2]
            [:test/n2 :naga/first 42]
            [:test/n2 :naga/rest :test/n3]
            [:test/n3 :naga/first 54]
            [:test/n3 :naga/rest :naga/nil]] m3))
    (is (= [[:test/n1 :prop "val"]
            [:test/n2 :prop "val2"]] m4))
    (is (= [[:test/n1 :prop "val"]
            [:test/n1 :arr :test/n2]
            [:test/n2 :naga/first :test/n3]
            [:test/n2 :naga/rest :test/n4]
            [:test/n3 :a 1]
            [:test/n4 :naga/first :test/n5]
            [:test/n4 :naga/rest :test/n6]
            [:test/n5 :a 2]
            [:test/n6 :naga/first :test/n7]
            [:test/n6 :naga/rest :naga/nil]
            [:test/n7 :naga/first "nested"]
            [:test/n7 :naga/rest :naga/nil]] m5))))
