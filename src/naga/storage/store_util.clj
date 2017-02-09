(ns ^{:doc "Some common utilities for storage functions"
      :author "Paula Gearon"}
    naga.storage.store-util
  (:require [schema.core :as s]
            [naga.schema.structs :as st :refer [EPVPattern Value]]
            [naga.store :as store]))

(s/defn project-row :- [s/Any]
  "Creates a new EPVPattern from an existing one, based on existing bindings.
   Uses the mapping to copy from columns in 'row' to overwrite variables in 'pattern'.
   'pattern' must be a vector.
   The index mappings have already been found and are in the 'mapping' argument"
  [storage
   wide-pattern :- [s/Any]
   nodes :- (s/maybe [s/Num])
   mapping :- {s/Num s/Num}
   row :- [Value]]
  (let [get-node (memoize (fn [n] (store/new-node storage)))
        node-statements (mapcat (fn [i]
                                  (let [node (get-node i)]
                                    [node :db/ident node]))
                                nodes)
        update-pattern (fn [p [t f]]
                         (let [v (if (neg? f) (get-node f) (nth row f))]
                           (assoc p t v)))]
    (concat node-statements
            (reduce update-pattern wide-pattern mapping))))
