(ns ^{:doc "Functions to run rules until completion."
      :author "Paula Gearon"}
    naga.engine
    (:require [naga.schema.structs :as st
               :refer
               #?(:clj [EPVPattern RulePatternPair
                        StatusMap StatusMapEntry Body Program]
                  :cljs [EPVPattern RulePatternPair StatusMap
                         StatusMapEntry Body Program Rule DynamicRule])]
              #?(:clj [naga.queue :as q]
                 :cljs [naga.queue :as q :refer [PQueue]])
              #?(:clj [naga.store :as store]
                 :cljs [naga.store :as store :refer [Storage]])
              [naga.util :as u]
              [schema.core :as s])
    #?(:clj
       (:import [naga.schema.structs Rule DynamicRule]
                [naga.store Storage]
                [naga.queue PQueue])))


(def true* (constantly true))


(s/defn extract-dirty-pattern :- (s/maybe EPVPattern)
  "Takes a key and value pair (from a status map) and determines if
  the value (a ConstraintData) is marked dirty.  If it is dirty, then return
  the key (an EPVPattern)."
  [[p status-atom] :- StatusMapEntry]
  (if (some-> status-atom deref :dirty)
    p))


(s/defn resolve-count :- (s/maybe EPVPattern)
  "Resolve a pattern against storage, and set the :resolution meta
  data if the result is different from the last resolution.  Requires
  a status map in order to lookup the last-count."
  [storage :- Storage
   status :- StatusMap
   p :- EPVPattern]
  (let [resolution-count (store/count-pattern storage p)
        last-count (:last-count @(get status p))]
    (when-not (= last-count resolution-count)
      (with-meta p {:count resolution-count}))))


(s/defn mark-rule-cleaned-with-latest-count!
  "Reset the pattern status, making it clean.  Uses meta from
   resolve-count (above). Result should be ignored."
  [dirty-patterns :- [EPVPattern]
   counted-set :- #{EPVPattern}
   status :- StatusMap]
  (doseq [dp dirty-patterns]
    (let [{c :count} (if-let [cp (get counted-set dp)]
                       (meta cp))

          pattern-status (get status dp)]
      (reset! pattern-status
              {:last-count (or c (:last-count @pattern-status))
               :dirty false}))))


(s/defn schedule-downstream-queue
  "Adds all downstream rules to the queue.
   The queue will adjust order according to salience, if necessary.
   Also marks relevant patterns in the downstream rule bodies as dirty."
  [rules :- {s/Str DynamicRule}
   remaining-queue :- PQueue
   downstream :- [RulePatternPair]]

  (reduce (fn [rqueue [rname pattern]]
            (let [{status :status :as sched-rule} (get rules rname)
                  constraint-data (get status pattern)]
              (when-not (list? pattern)
                (assert constraint-data
                        (str "rule-constraint pair missing in rule: " rname))
                (swap! constraint-data update-in [:dirty] true*))
              (q/add rqueue sched-rule)))
          remaining-queue
          downstream)) ;; contains rule-name/pattern pairs for update


(s/defn execute :- [(s/one Storage "Final value of storage")
                    (s/one {s/Str s/Num} "Map of rule name to execution count")]
  "Executes a program. Data is retrieved from and inserted into db-store."
  [rules :- {s/Str DynamicRule}
   db-store :- Storage]
  #?(:cljs (js/alert rules))
  #?(:cljs (js/alert db-store))
  (let [rule-queue (reduce q/add
                           (q/new-queue :salience :name)
                           (vals rules))]
    (loop [queue rule-queue
           storage db-store]
      (let [{:keys [status body head downstream execution-count]
             :as current-rule} (q/head queue)

            remaining-queue (q/pop queue)]
        #?(:cljs (js/alert (pr-str current-rule)))
        #?(:cljs (js/alert (pr-str remaining-queue)))

        (if (nil? current-rule)
          ;; finished, build results as rule names mapped to how often
          ;; the rule was run
          [storage
           (u/mapmap :name (comp deref :execution-count) (vals rules))]


          ;; find if any patterns have updated
          (if-let [dirty-patterns (seq (keep extract-dirty-pattern
                                             status))]
            ;; rule needs to be run
            (let [counted-patterns (keep (partial resolve-count storage status)
                                         dirty-patterns)

                  counted-set (set counted-patterns)

                  hinted-patterns (map #(get counted-set % %) body)]

              (mark-rule-cleaned-with-latest-count! dirty-patterns
                                                    counted-set
                                                    status)

              #?(:cljs (js/alert (str "counted-patterns: " (pr-str (seq counted-patterns)))))
              ;; is there a NEW result to be had?
              (if (seq counted-patterns)
                ;; TODO: EXECUTE ACTIONS FOR ACTION RULES
                ;; (if (= rule-type :action)
                ;;   (if-let [data (seq (store/query storage head hinted-patterns))]
                ;;     (do-action data))
                ;;     ;; ^ empty data means no action
                ;;   :else ...)
                ;; insert data according to the rule

                (let [updated-storage (store/query-insert storage
                                                          head
                                                          hinted-patterns)
                      scheduled-queue (schedule-downstream-queue rules
                                                                 remaining-queue
                                                                 downstream)]
                  (swap! execution-count inc)
                  (recur scheduled-queue updated-storage))

                ;; no new results, so move to next
                (recur remaining-queue storage)))

            ;; no dirty patterns, so rule did not need to be run
            (recur remaining-queue storage)))))))

(s/defn initialize-rules :- {s/Str DynamicRule}
  "Takes rules with calculated dependencies, and initializes them"
  [rules :- {s/Str Rule}]
  (letfn [(init-rule [{:keys [head body name salience downstream]}]
            (st/new-rule head body name downstream salience
                         (u/mapmap (fn [_]
                                     (atom {:last-count 0
                                            :dirty true}))
                                   (remove list? body))
                         (atom 0)))]
    (into {} (map (fn [[rule-name rule]]
                    [rule-name (init-rule rule)])
                  rules))))

(s/defn run :- [(s/one Storage "Resulting data store")
                (s/one {s/Str s/Num} "Execution stats")
                (s/one (s/maybe {s/Str s/Num}) "Execution stats")]
  "Runs a program against a given configuration"
  [config :- {s/Keyword s/Any}
   {:keys [rules axioms]} :- Program]
  (let [storage (store/get-storage-handle config)
        storage' (store/start-tx storage)
        rules' (initialize-rules rules)
        initialized-storage (store/assert-data storage' axioms)
        [output-storage stats] (execute rules' initialized-storage)
        result-storage (store/commit-tx output-storage)
        delta-ids (store/deltas result-storage)]
    [result-storage stats delta-ids]))
