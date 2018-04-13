(ns naga.test-pabu
  (:require
            #?(:clj  [naga.rules :as r :refer [r rule]]
               :cljs [naga.rules :as r :refer [rule] :refer-macros [r]])
            [naga.lang.pabu :refer [read-str]]
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]])
            #?(:clj  [clojure.test :as t :refer [is]]
               :cljs [clojure.test :as t :refer-macros [is]])
            [clojure.pprint :refer [pprint]]))

(t/use-fixtures :once st/validate-schemas)

(def program-string
  "sibling(fred, barney).
parent(fred, mary).
sibling(mary, george).
gender(george, male).

parent(B, C) :- sibling(A, B), parent(A, C).
brother(A, B) :- sibling(A, B), gender(B, male).
uncle(A, C) :- parent(A, B), brother(B, C).
/* sibling(A, B) :- parent(A, P), parent(B, P). */
sibling(A, B) :- parent(A, P), parent(B, P), A != B.
gender(F, male) :- father(A, F).
parent(A, F) :- father(A, F).
")

(def test-rules
  [(r [?b :parent ?c] :- [?a :sibling ?b] [?a :parent ?c])
   (r [?a :brother ?b] :- [?a :sibling ?b] [?b :gender :male])
   (r [?a :uncle ?c] :- [?a :parent ?b] [?b :brother ?c])
   (rule '[[?a :sibling ?b]]  ['[?a :parent ?p] '[?b :parent ?p] (list not= '?a '?b)])
   (r [?f :gender :male] :- [?a :father ?f])
   (r [?a :parent ?f] :- [?a :father ?f])])

(def test-axioms
  [[:fred :sibling :barney]
   [:fred :parent :mary]
   [:mary :sibling :george]
   [:george :gender :male]])

(defn- unord=
  "Compares the contents of 2 sequences, with ordering not considered."
  [a b]
  (= (set a) (set b)))

(def clean (map #(dissoc % :name :status :execution-count)))

(deftest parse-axiom
  (let [{:keys [axioms rules]} (read-str "foo(bar, baz).")]
    (is (= [[:bar :foo :baz]] axioms))
    (is (empty? rules))))

(deftest parse-rule
  (let [{:keys [axioms rules]} (read-str "bar(baz, Y) :- foo(X, baz).")]
    (is (empty? axioms))
    (is (= (sequence clean [(r [:baz :bar ?y] :- [?x :foo :baz])])
           (sequence clean rules)))))

(deftest parse-program
  (let [{:keys [rules axioms]} (read-str program-string)]
    (is (= test-axioms axioms))
    (is (= (sequence clean test-rules)
           (sequence clean rules)))))

#?(:cljs (t/run-tests))
