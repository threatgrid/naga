(ns naga.engine
  (:require [naga.structs :as st :refer [EPVPattern RulePatternPair
                                         StatusMap StatusMapEntry Body Program]]
            [naga.queue :as q]
            [naga.store :as store]
            [naga.util :as u]
            [schema.core :as s])
  (:import [naga.structs Rule]
           [naga.store Storage]
           [naga.queue PQueue]))


(def true* (constantly true))


(s/defn extract-dirty-pattern :- EPVPattern
  "Takes a key and value pair (from a status map) and determines if
  the value (a ConstraintData) is marked dirty.  If it is dirty, then return
  the key (an EPVPattern)."
  [[p status-atom] :- StatusMapEntry]
  (if (some-> status-atom deref :dirty)
    p))


(s/defn resolve-pattern :- (s/maybe EPVPattern)
  "Resolve a pattern against storage, and set the :resolution meta
  data if the result is different from the last resolution.  Requires
  a status map in order to lookup the last-count."
  [storage :- Storage
   status :- StatusMap
   p :- EPVPattern]
  (let [resolution (store/resolve storage p)
        last-count (:last-count @(get status p))]
    (when-not (= last-count (count resolution))
      (with-meta p {:resolution resolution}))))


(s/defn mark-rule-cleaned-with-latest-count!
  "Reset the pattern status, making it clean.  Uses meta from
   resolve-pattern (above). Result should be ignored."
  [dirty-patterns :- [EPVPattern]
   resolved-set :- #{EPVPattern}
   status :- StatusMap]
  (doseq [dp dirty-patterns]
    (let [{r :resolution} (if-let [rp (get resolved-set dp)]
                            (meta rp))

          pattern-status (get status dp)]
      (reset! pattern-status
              {:last-count (if r
                             (count r)
                             (:last-count @pattern-status))
               :dirty false}))))


(s/defn schedule-downstream-queue
  "Adds all downstream rules to the queue.
   The queue will adjust order according to salience, if necessary.
   Also marks relevant patterns in the downstream rule bodies as dirty."
  [rules :- {s/Str Rule}
   remaining-queue :- PQueue
   downstream :- [RulePatternPair]]

  (reduce (fn [rqueue [rname pattern]]
            (let [{status :status :as sched-rule} (get rules rname)
                  constraint-data (get status pattern)]
              (assert constraint-data
                      (str "rule-constraint pair missing in rule: " rname))
              (swap! constraint-data update-in [:dirty] true*)
              (q/add rqueue sched-rule)))
          remaining-queue
          downstream)) ;; contains rule-name/pattern pairs for update


(s/defn execute
  "Executes a program. Data is retrieved from and inserted into db-store."
  [rules :- {s/Str Rule}
   db-store :- Storage]
  (let [rule-queue (reduce q/add
                           (q/new-queue :salience :name)
                           (vals rules))]
    (loop [queue rule-queue
           storage db-store]
      (let [{:keys [status body head downstream execution-count]
             :as current-rule} (q/head queue)

            remaining-queue (q/pop queue)]

        (if (nil? current-rule)
          ;; finished, build results as rule names mapped to how often
          ;; the rule was run
          (u/mapmap :name (comp deref :execution-count) (vals rules))

          ;; find if any patterns have updated
          (if-let [dirty-patterns (seq (keep extract-dirty-pattern
                                             status))]
            ;; rule needs to be run
            (let [resolved-patterns (keep (partial resolve-pattern storage status)
                                          dirty-patterns)

                  resolved-set (into #{} resolved-patterns)

                  hinted-patterns (map #(get resolved-set % %) body)]

              (mark-rule-cleaned-with-latest-count! dirty-patterns
                                                    resolved-set
                                                    status)

              ;; is there a NEW result to be had?
              (if (seq resolved-patterns)
                ;; TODO: EXECUTE ACTIONS FOR ACTION RULES
                ;; (if (= rule-type :action)
                ;;   (do-action (store/query storage head hinted-patterns))
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

(s/defn run :- s/Bool
  "Runs a program against a given configuration"
  [config :- {s/Keyword s/Any}
   {:keys [rules axioms]} :- Program]
  (let [storage (store/get-storage-handle config)]
    (store/assert-data storage axioms)
    (execute rules storage)))
