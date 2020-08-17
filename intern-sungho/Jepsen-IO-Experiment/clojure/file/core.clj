(ns tutorial.core
  (:gen-class)) ; namespace


(defn -main [& args]

  (spit "example.txt" "First Sentence\n")

  (with-open [w (clojure.java.io/writer "example.txt" :append true)]
    (.write w (str "Second " "Sentence")))

  (def string1 (slurp "example.txt"))
  (println string1)

  (with-open [rdr (clojure.java.io/reader "example.txt")]
    (reduce conj [] (line-seq rdr)))

  (println (.exists (clojure.java.io/file "example.txt")))

)


