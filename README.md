# BE_Project_final
Automatic Notification Bot Using Web Mining for Event Analysis  

Steps for program execution:  
On terminal 1  
1> export FLASK_APP=handlerservice.py  
2> flask run  

On terminal 2  
1> nohup ~/Downloads/kafka/bin/kafka-server-start.sh ~/Downloads/kafka/config/server.properties > ~/Downloads/kafka/kafka.log 2>&1 &  
2> Open eclipse with root priviledges  
3> Run ReadFromKafka.java  
4> python scraper_sim_trial.py /home/mausam/2016-10-12-South\ Africa-Australia.csv //for simulation  
5> python scraper.py http://www.espncricinfo.com/west-indies-v-england-2016-17/engine/match/1027315.html [match page url from espncricinfo] //for live matches  
