grep -rn "SELECT status FROM kanban_cards WHERE id = ?" policies/ | awk -F: '{print $1":"$2}'
