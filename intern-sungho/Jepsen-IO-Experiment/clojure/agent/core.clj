(ns tutorial.core
  (:gen-class)) ; namespace

(defn -main [& args]

  (println "Create a counter with agent")
  (def counter (agent 0))
  (println counter)
  (println @counter)

  (println "\nIncrementing Counter but fail to read")
  (send counter + 100)
  (println @counter)

  (println "\nIncrementing Counter and read")
  (send-off counter + 100)
  (println @counter)
  (println @counter)

  (println "\nIncrementing counter and wait for the counter to be 200")
  (println (await-for 200 counter))
  (println @counter)

  (println "\nIncrementing Counter and wait for the update in the counter")
  (send-off counter + 100)
  (await counter)
  (println @counter)

  (println "\nCheck the updates of the variable x with add-watch")
  (def x (atom 0))
  (add-watch x :watcher
             (fn [key atom old-state new-state]
               (println "The value of the atom has been changed")
               (println "old-state" old-state)
               (println "new-state" new-state)))
  (reset! x 2) ; just updated the data

  (shutdown-agents)

)