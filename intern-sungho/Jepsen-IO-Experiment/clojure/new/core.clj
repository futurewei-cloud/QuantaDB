(ns tutorial.core
  (:gen-class)) ; namespace

(defn -main [& args]

  (println "assoc BEGIN")
  (def test1 {})
  (assoc test1 :key1 "value" :key2 "another value")
  (println "assoc empty hashmap: " test1)

  (def test2 {:account-no 12345678 :lname "Jones" :fnam "Fred"})
  (def modified (assoc test2 :fnam "Sue"))
  (println "before assoc" test2)
  (println "after assoc" modified)

  (println "\nmap")
  (def animals ["pig" "cow" "goat" "cat" "dog" "rabbit"])
  (defn pluralise
    "Pluralise a given string value"
    [animal]
    (str animal "s"))
    (println animals)
    (println(map pluralise animals))

)