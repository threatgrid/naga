(ns naga.storage.test-memory
  (:require [naga.storage.memory :refer :all]
            [naga.store :refer :all]
            [clojure.test :refer :all]
            [schema.test :as st]))

(use-fixtures :once st/validate-schemas)

(def data
  [[:a :p1 :x]
   [:a :p1 :y]
   [:a :p2 :z]
   [:a :p3 :x]
   [:b :p1 :x]
   [:b :p2 :x]
   [:b :p3 :z]
   [:c :p4 :t]])

(defn unordered-resolve
  [g pattern]
  (into #{} (resolve-pattern g pattern)))

(deftest test-load
  (let [s (assert-data new-store data)
        r1 (unordered-resolve s '[:a ?a ?b])
        r2 (unordered-resolve s '[?a :p2 ?b])
        r3 (unordered-resolve s '[:a :p1 ?a])
        r4 (unordered-resolve s '[?a :p2 :z])
        r5 (unordered-resolve s '[:a ?a :x])
        r6 (unordered-resolve s '[:a :p4 ?a])]
    (is (= #{[:p1 :x]
             [:p1 :y]
             [:p2 :z]
             [:p3 :x]} r1))
    (is (= #{[:a :z]
             [:b :x]} r2))
    (is (= #{[:x]
             [:y]} r3))
    (is (= #{[:a]} r4))
    (is (= #{[:p1]
             [:p3]} r5))
    (is (empty? r6))))

(def jdata
  [[:a :p1 :x]
   [:a :p1 :y]
   [:a :p2 :z]
   [:a :p3 :x]
   [:b :p1 :x]
   [:b :p2 :x]
   [:b :p3 :z]
   [:c :p4 :t]
   [:x :q1 :l]
   [:x :q2 :m]
   [:y :q1 :l]
   [:y :q3 :n]])

(defn unordered-join
  [s op pattern]
  (into #{} (join s op pattern)))

(deftest test-join
  (let [s (assert-data new-store jdata)
        r1 (unordered-join s '[?a ?b ?d] '[[:a ?a ?b] [?b ?c ?d]])
        r2 (unordered-join s '[?a ?b ?d] '[[?a ?b :x] [:a ?b ?d]])]
    (is (= #{[:p1 :x :l]
             [:p1 :x :m]
             [:p1 :y :l]
             [:p1 :y :n]
             [:p3 :x :l]
             [:p3 :x :m]} r1))
    (is (= #{[:a :p1 :x]
             [:a :p1 :y]       
             [:a :p3 :x]
             [:b :p1 :x]
             [:b :p1 :y]
             [:b :p2 :z]} r2))
    ))
