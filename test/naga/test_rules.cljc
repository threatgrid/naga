(ns naga.test-rules
  (:require [naga.rules :as r :refer [r]]
            [naga.schema.structs :as structs :refer [new-rule]]
            [naga.engine :as e]
            [naga.store :as store]
            [naga.store-registry :as store-registry]
            [naga.storage.test :as stest]
            [asami.core :as mem]
            [schema.test :as st]
            [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]))

(use-fixtures :once st/validate-schemas)

(def rules
  [(r "shared-parent" [?b :parent ?c] :- [?a :sibling ?b] [?a :parent ?c])
   (r "sibling->brother" [?a :brother ?b] :- [?a :sibling ?b] [?b :gender :male])
   (r "uncle" [?a :uncle ?c] :- [?a :parent ?b] [?b :brother ?c])
   ;; (r "siblings" [?a :sibling ?b] :- [?a :parent ?p] [?b :parent ?p] (not= ?a ?b))
   (r "male-father" [?f :gender :male] :- [?a :father ?f])
   (r "parent-father" [?a :parent ?f] :- [?a :father ?f])])

(def axioms
  [[:fred :sibling :barney]
   [:fred :parent :mary]
   [:mary :sibling :george]
   [:george :gender :male]])

(defn- unord=
  "Compares the contents of 2 sequences, with ordering not considered."
  [a b]
  (= (set a) (set b)))

(deftest build-program
  (let [{program :rules} (r/create-program rules [])]
    (is (= (count rules) (count program)))
    (is (unord= (map first (get-in program ["shared-parent" :downstream]))
                ["shared-parent" "uncle"]))
    (is (unord= (map first (get-in program ["sibling->brother" :downstream]))
                ["uncle"]))
    (is (unord= (map first (get-in program ["uncle" :downstream]))
                []))
    (is (unord= (map first (get-in program ["male-father" :downstream]))
                ["sibling->brother"]))
    (is (unord= (map first (get-in program ["parent-father" :downstream]))
                ["shared-parent" "uncle"]))))


(deftest single-rule
  (let [ptn '[?a :ancestor ?b]
        r1 (new-rule '[[?a :parent ?b]] [ptn] "stub1" [])
        r2 (new-rule '[[?a :parent ?b]] [ptn] "stub2" [["stub2" ptn]])
        p1 {:rules (e/initialize-rules {"stub1" r1}) :axioms []}
        p2 {:rules (e/initialize-rules {"stub2" r2}) :axioms []}]
    (let [[_ name->count] (e/execute (:rules p1) stest/empty-store)]
      (is (= 1 (name->count "stub1"))))
    (let [[_ name->count] (e/execute (:rules p2) stest/empty-store)]
      (is (= 4 (name->count "stub2"))))))

(deftest run-family
  (store-registry/register-storage! :memory mem/create-store)
  (let [program (r/create-program rules axioms)
        [store results] (e/run {:type :memory} program)
        unk (store/resolve-pattern store '[?n :uncle ?u])]
    (is (= 2 (count unk)))
    (is (= #{[:fred :george] [:barney :george]} (set unk))))

  (let [fresh-store (store-registry/get-storage-handle {:type :memory})
        original-store (store/assert-data fresh-store axioms)
        config {:type :memory :store original-store}
        program (r/create-program rules [])
        [store results] (e/run config program)
        unk (store/resolve-pattern store '[?n :uncle ?u])]
    (is (= 2 (count unk)))
    (is (= #{[:fred :george] [:barney :george]} (set unk)))))

(deftest multi-prop
  (store-registry/register-storage! :memory mem/create-store)
  (let [r2 [(r "multi-prop" [?x :first :a] [?x :second :b] :- [?x :foo ?y])]
        a2 [[:data :foo :bar]]
        program (r/create-program r2 a2)
        [store results] (e/run {:type :memory} program)
        data (store/resolve-pattern store '[?e ?a ?v])]
    (is (= 3 (count data)))
    (is (every? #{:data} (map first data)))
    (is (= #{[:data :foo :bar] [:data :first :a] [:data :second :b]} (set data)))))

(deftest blank-prop
  (store-registry/register-storage! :memory mem/create-store)
  (let [rx [(r "multi-prop" [?z :first :a] :- [?x :foo ?y])]
        ax [[:data :foo :bar]]
        program (r/create-program rx ax)
        [store results] (e/run {:type :memory} program)
        data (store/resolve-pattern store '[?e ?a ?v])
        data' (remove #(= :data (first %)) data)]
    (is (= 4 (count data)))
    (is (= 3 (count data')))
    (is (not= :data (ffirst data')))
    (is (apply = (map first data')))))

(deftest blank-multi-prop
  (store-registry/register-storage! :memory mem/create-store)
  (let [rx [(r "multi-prop" [?z :first :a] [?z :second ?y] :- [?x :foo ?y])]
        ax [[:data :foo :bar]]
        program (r/create-program rx ax)
        [store results] (e/run {:type :memory} program)
        data' (store/resolve-pattern store '[?e ?a ?v])
        data (remove #(= [:data :foo :bar] %) data')
        node* (map first data)
        node (first node*)]
    (is (= 4 (count data)))
    (is (apply = node*))
    (is (some #(= [node :first :a] %) data))
    (is (some #(= [node :second :bar] %) data))
    (is (some #(= [node :db/ident (store/node-label store node)] %) data)))
  (let [rx [(r "multi-prop" [?z :first :a] [?z :second ?y]
               [?a :first :b] [?a :third ?x] :- [?x :foo ?y])]
        ax [[:data :foo :bar]]
        program (r/create-program rx ax)
        [store results] (e/run {:type :memory} program)
        data' (store/resolve-pattern store '[?e ?a ?v])
        data (remove #(= [:data :foo :bar] %) data')
        nodes (set (map first data))]
    (is (= 8 (count data)))
    (is (= 2 (count nodes)))
    (is (some (fn [[e a v]] (and (nodes e) (= [:first :a] [a v]))) data))
    (is (some (fn [[e a v]] (and (nodes e) (= [:first :b] [a v]))) data))
    (is (some (fn [[e a v]] (and (nodes e) (= [:second :bar] [a v]))) data))
    (is (some (fn [[e a v]] (and (nodes e) (= [:third :data] [a v]))) data))
    (is (= 2 (count
              (filter (fn [[e a v]]
                        (and (nodes e)
                             (= :db/ident a)
                             (= (store/node-label store e) v)))
                      data)))))
  (let [rx [(r "multi-prop" [?z :first :a] [?z :second ?y] :- [?x :foo ?y])]
        ax [[:data :foo :bar] [:other :foo :baz]]
        program (r/create-program rx ax)
        [store results] (e/run {:type :memory} program)
        data' (store/resolve-pattern store '[?e ?a ?v])
        data (remove #(#{[:data :foo :bar] [:other :foo :baz]} %) data')
        nodes (set (map first data))]
    (is (= 8 (count data)))
    (is (= 2 (count nodes)))
    (is (= 2 (count (filter (fn [[e a v]] (and (nodes e) (= [:first :a] [a v]))) data))))
    (is (= 1 (count (filter (fn [[e a v]] (and (nodes e) (= [:second :bar] [a v]))) data))))
    (is (= 1 (count (filter (fn [[e a v]] (and (nodes e) (= [:second :baz] [a v]))) data))))
    (is (= 2 (count (filter (fn [[e a v]] (and (nodes e) (= :db/ident a) (= (store/node-label store e) v))) data))))))


(deftest loop-breaking
  (store-registry/register-storage! :memory mem/create-store)
  (let [rx [(r "multi-prop" [?z :first :a] [?z :second ?y] :- [?x :foo ?y])
            (r "bad-loop" [?x :foo ?b] :- [?a :first ?b])] ;; gets run once!
        ax [[:data :foo :bar]]
        program (r/create-program rx ax)
        [store results] (e/run {:type :memory} program)
        data' (store/resolve-pattern store '[?e ?a ?v])
        data (remove #(= [:data :foo :bar] %) data')
        node (set (map first data))]
    (is (= 7 (count data)))
    (is (= 2 (count node)))))

(defn short-rule
  [{:keys [head body]}]
  (concat head [:-] body))

(defn demo-family
  []
  (store-registry/register-storage! :memory mem/create-store)

  (let [program (r/create-program rules axioms)
        [store results] (e/run {:type :memory} program)
        unk (store/resolve-pattern store '[?n :uncle ?u])]

    (println "ORIGINAL DATA")
    (pprint axioms)

    (println "RULES")
    (pprint (map short-rule rules))

    (println "OUTPUT DATABASE")
    (pprint (store/query store '[?e ?p ?v] '[[?e ?p ?v]]))

    (println "Uncle data")
    (pprint unk)

    (println)
    (pprint results)
    ))
