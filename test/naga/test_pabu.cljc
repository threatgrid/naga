(ns naga.test-pabu
  (:require
            #?(:clj  [naga.rules :as r :refer [r rule]]
               :cljs [naga.rules :as r :refer [rule rule->str] :refer-macros [r]])
            [naga.lang.pabu :refer [read-str rule->str]]
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]])
            #?(:clj  [clojure.test :as t :refer [is]]
               :cljs [clojure.test :as t :refer-macros [is]])
            [clojure.pprint :refer [pprint]]))

(t/use-fixtures :once st/validate-schemas)

(def program-string
  "-- a test program
%these are the axioms
sibling(fred, barney).
parent(fred, mary).
sibling(mary, george).
gender(george, male).

-- followed by the rules
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
   (rule '[[?a :sibling ?b]]  ['[?a :parent ?p] '[?b :parent ?p] [(list not= '?a '?b)]])
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

(deftest parse-literal
  (let [{:keys [axioms rules]} (read-str "foo(bar, 3).")]
    (is (= [[:bar :foo 3]] axioms))
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

(deftest parse-program-with-contains
  (read-str "p(A, B) :- naga:contains(A, B)."))

(deftest emit-program
  (let [[r1 r2 r3 r4 r5 r6] (map rule->str test-rules)
        rn (rule->str (r "a-name" [?a :foo ?b] :- [?a :bar ?b]) true)]
    (is (= r1 "parent(B, C) :- sibling(A, B), parent(A, C)."))
    (is (= r2 "brother(A, B) :- sibling(A, B), gender(B, male)."))
    (is (= r3 "uncle(A, C) :- parent(A, B), brother(B, C)."))
    (is (= r4 "sibling(A, B) :- parent(A, P), parent(B, P), A != B."))
    (is (= r5 "gender(F, male) :- father(A, F)."))
    (is (= r6 "parent(A, F) :- father(A, F)."))
    (is (= rn "foo(A, B) :- bar(A, B).    /* a-name */"))))

(def suggestion-rule
  "module(Odns, “OpenDNS”),\nblock(Odns, O),\ntype(Odns, “suggestion”)\n:-\ntype(M, “module”), record(M, “opendns-investigate.module/OpenDNSInvestigateModule”),\ntype(V, “verdict”), disposition_name(V, “Malicious”),\nobservable(V, O), type(O, “domain”),\nmodule-name(V, Mn), Mn != “OpenDNS” .")

(deftest parse-suggestion-rule-program
  (let [{:keys [axioms rules]} (read-str suggestion-rule)
        [{:keys [head body]}] rules
        pred (last body)]
    (is (= 1 (count rules)))
    (is (= 3 (count head)))
    (is (= 8 (count body)))
    (is (list? (first pred)))
    (is (= [(list not= '?mn "OpenDNS")] pred))))

#?(:cljs (t/run-tests))
