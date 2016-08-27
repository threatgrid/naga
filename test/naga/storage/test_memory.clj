(ns naga.storage.test-memory
  (:require [naga.storage.memory.core :refer :all]
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
  (let [s (assert-data empty-store data)
        r1 (unordered-resolve s '[:a ?a ?b])
        r2 (unordered-resolve s '[?a :p2 ?b])
        r3 (unordered-resolve s '[:a :p1 ?a])
        r4 (unordered-resolve s '[?a :p2 :z])
        r5 (unordered-resolve s '[:a ?a :x])
        r6 (unordered-resolve s '[:a :p4 ?a])
        r7 (unordered-resolve s '[:c :p4 :t])]
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
    (is (empty? r6))
    (is (= #{[]} r7))))

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

(defn unordered-query
  [s op pattern]
  (into #{} (query s op pattern)))

(deftest test-join
  (let [s (assert-data empty-store jdata)
        r1 (unordered-query s '[?a ?b ?d] '[[:a ?a  ?b] [?b ?c  ?d]])
        r2 (unordered-query s '[?a ?b ?d] '[[?a ?b  :x] [:a ?b  ?d]])
        r3 (unordered-query s '[?x ?y]    '[[:a :p1 ?x] [:y :q1 ?y]])
        r4 (unordered-query s '[?x]       '[[:a :p1 ?x] [:y :q1 :l]])
        r5 (unordered-query s '[?x ?y]    '[[:a :p1 ?x] [:y ?y  ?z]])]
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
    (is (= #{[:x :l]
             [:y :l]} r3))
    (is (= #{[:x]
             [:y]} r4))
    (is (= #{[:x :q1]
             [:x :q3]
             [:y :q1]
             [:y :q3]} r5))))


(def j2data
  [[:a :p1 :b]
   [:a :p1 :b]
   [:a :p2 :z]
   [:a :p3 :x]
   [:b :px :c]
   [:b :px :d]
   [:b :py :c]
   [:c :pa :t]
   [:c :pb :u]
   [:d :pz :l]
   [:x :q2 :m]
   [:y :q1 :l]
   [:y :q3 :n]])

(deftest test-multi-join
  (let [s (assert-data empty-store j2data)
        r1 (unordered-query s '[?p ?v] '[[:a ?a ?b] [?b :px ?d] [?d ?p ?v]])]
    (is (= #{[:pa :t]
             [:pb :u]
             [:pz :l]} r1))))
