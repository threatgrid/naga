(ns naga.storage.test-query
  "Tests internals of the query portion of the memory storage"
  (:require [naga.storage.memory.core :refer :all]
            [naga.storage.memory.index :refer [Graph]]
            [naga.store :refer :all]
            [naga.util :refer [mapmap]]
            [clojure.test :refer :all]
            [schema.test :as st]))

(use-fixtures :once st/validate-schemas)

(defn mapto [s1 s2]
  (into {} (filter second (map vector s1 s2))))

(deftest test-query-paths
  (let [short-patterns '[[?a :b ?c]
                         [?d :e :f]
                         [?c ?d ?e]]
        path1 (min-join-path short-patterns
                             (mapto short-patterns [1 2 3]))
        path2 (min-join-path short-patterns
                             (mapto short-patterns [2 1 3]))
        path3 (min-join-path short-patterns
                             (mapto short-patterns [2 3 1]))
        path4 (min-join-path short-patterns
                             (mapto short-patterns [3 2 1]))]
    (is (= '[[?a :b ?c]
             [?c ?d ?e]
             [?d :e :f]]
           path1))
    (is (= '[[?d :e :f]
             [?c ?d ?e]
             [?a :b ?c]]
           path2))
    (is (= '[[?c ?d ?e]
             [?a :b ?c]
             [?d :e :f]]
           path3))
    (is (= '[[?c ?d ?e]
             [?d :e :f]
             [?a :b ?c]]
           path4))))


(defrecord StubResolver [counts]
  Graph
  (graph-add [this _ _ _] this)
  (graph-delete [this _ _ _] this)
  (resolve-triple [store s p o] (repeat (get counts [s p o])
                                        [:s :p :o])))

(defn resolver-for [patterns counts]
  (let [m (mapto patterns counts)]
    (->StubResolver m)))

(deftest test-filtered-query-paths
  (let [short-patterns [(with-meta '(= ?d 5) {:vars '#{?d}})
                        '[?a :b ?c]
                        '[?d :e :f]
                        (with-meta '(not= ?e ?a) {:vars '#{?e ?a}})
                        '[?c ?d ?e]]
        [path1] (plan-path (resolver-for short-patterns [nil 1 2 nil 3])
                           short-patterns [])
        [path2] (plan-path (resolver-for short-patterns [nil 2 1 nil 3])
                             short-patterns [])
        [path3] (plan-path (resolver-for short-patterns [nil 2 3 nil 1])
                           short-patterns [])
        [path4] (plan-path (resolver-for short-patterns [nil 3 2 nil 1])
                           short-patterns [])]
    (is (= '[[?a :b ?c]
             [?c ?d ?e]
             (= ?d 5)
             (not= ?e ?a)
             [?d :e :f]]
           path1))
    (is (= '[[?d :e :f]
             (= ?d 5)
             [?c ?d ?e]
             [?a :b ?c]
             (not= ?e ?a)]
           path2))
    (is (= '[[?c ?d ?e]
             (= ?d 5)
             [?a :b ?c]
             (not= ?e ?a)
             [?d :e :f]]
           path3))
    (is (= '[[?c ?d ?e]
             (= ?d 5)
             [?d :e :f]
             [?a :b ?c]
             (not= ?e ?a)]
           path4))))

(deftest var-mapping
  (let [m1 (matching-vars `[?a :rel ?c] `[?a ?b ?c] )
        m2 (matching-vars `[?b :rel ?f] `[?a ?b ?c ?d ?e ?f])
        m3 (matching-vars `[?b :rel ?f ?b :r2 ?e] `[?a ?b ?c ?d ?e ?f])
        m4 (matching-vars `[?x :rel ?f ?x :r2 ?e] `[?a ?b ?c ?d ?e ?f])]
    (is (= m1 {0 0, 2 2}))
    (is (= m2 {0 1, 2 5}))
    (is (= m3 {0 1, 2 5, 3 1, 5 4}))
    (is (= m4 {2 5, 5 4}))))
