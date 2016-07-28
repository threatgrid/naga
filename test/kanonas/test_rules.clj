(ns kanonas.test-rules
  (:require [kanonas.rules :as r :refer [r]]
            [schema.test :as st]
            [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]))

(use-fixtures :once st/validate-schemas)

(def rules
  [(r "shared-parent" [?b :parent ?c] :- [?a :sibling ?b] [?a :parent ?c])
   (r "sibling->brother" [?a :brother ?b] :- [?a :sibling ?b] [?b :gender :male])
   (r "uncle" [?a :uncle ?c] :- [?a :parent ?b] [?b :brother ?c])
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
  (let [program (r/create-program rules)]
     (println "PROGRAM:")
     (pprint program)
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
