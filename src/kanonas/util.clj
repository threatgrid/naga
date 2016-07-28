(ns kanonas.util
  "The ubiquitous utility namespace that every project seems to have")

(defn mapmap
  "Creates a map from functions applied to a seq.
   (map (partial * 2) [1 2 3 4 5])
     => {1 2, 2 4, 3 6, 4 8, 5 10}
   (map #(keyword (str \"k\" (dec %))) (partial * 3) [1 2 3])
     => {:k0 3, :k1 6, :k2 9}"
  ([keyfn s] (mapmap keyfn identity s))
  ([keyfn valfn s]
    (into {} (map (fn [e] [(keyfn e) (valfn e)]) s))))
