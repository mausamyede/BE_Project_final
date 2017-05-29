import sys
import time
import os
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import linecache

def tweet(post):
	mod_post=post.replace(" ","%20")
	mod_post=mod_post.replace("\r","%20")
	mod_post=mod_post.replace("\t","%20")
	mod_post=mod_post.replace("\n","%0A")
	#print mod_post
	status=os.system('curl -i http://localhost:5000/social_media/handler/'+mod_post+'/1')
	while status!=0:
		continue

now = datetime.datetime.now()
log=open('/home/mausam/crawlerlog.txt','a')
teamindex={'IND':'0','SL':'1','NZ':'2','AUS':'3','PAK':'4','BAN':'5','ENG':'6','WI':'7','SA':'8'}
teamanno={'India':'IND','Sri Lanka':'SL','New Zealand':'NZ','Australia':'AUS','Pakistan':'PAK','Bangladesh':'BAN','England':'ENG','West Indies':'WI','South Africa':'SA'}
rn=0
wn=0
rnminus1=0
wnminus1=0
runs=0
wicks=0
match_file_path=str(sys.argv[1])
matchinfo=linecache.getline(match_file_path,7).split(',')[2].rstrip("\n")
print matchinfo
tweet(matchinfo)
log.write(matchinfo+"\n")
venue=linecache.getline(match_file_path,10).split(',')[2].rstrip("\n")
print venue
venue_clus="1"
vfile=open('/home/mausam/venues.txt','r')
for vline in vfile:
	if vline.split(',')[0]==venue:
		venue_clus=vline.split(',')[1].rstrip('\n')
vfile.close()
team1=linecache.getline(match_file_path,2).split(',')[2].rstrip("\n")
team2=linecache.getline(match_file_path,3).split(',')[2].rstrip("\n")
target="0"
prev_seg=0
curr_bmen=[["" for x in range(2)] for y in range(2)]
prev_half=[]
prev_full=[]
inn1graph=0
prev_line=""
filename=str(now.year)+"-"+str(now.month)+"-"+str(now.day)+"-"+team1+"-"+team2+".txt"
f=open('/home/mausam/newmatch/'+filename,'a')
toss_winner=linecache.getline(match_file_path,11).split(',')[2].rstrip('\n')
toss_decision=linecache.getline(match_file_path,12).split(',')[2].rstrip('\n')
print toss_winner,' won the toss and decided to ',toss_decision
tweet(toss_winner+" won the toss and decided to "+toss_decision)
time.sleep(2)
log.write(toss_winner+" won the toss and decided to "+toss_decision+"\n")
match_file=open(match_file_path,'r')
for line in match_file:
	time.sleep(2)
	# get overs, balls, bats1, bats2, and resp ids
	if line.split(',')[0]!="ball":
		prev_line=line
		continue
	overs=line.split(',')[2].split('.')[0]
	balls=line.split(',')[2].split('.')[1]
	bats1=line.split(',')[4]
	bats2=line.split(',')[5]
	bats1clusID="3" 
	bats2clusID="3"
	bfile=open('/home/mausam/batsmen_cluster.csv','r')
	for bline in bfile:
		if bline.split(',')[0]==bats1:
			bats1clusID=bline.split(',')[2].rstrip('\n')
		if bline.split(',')[0]==bats2:
			bats2clusID=bline.split(',')[2].rstrip('\n')
	bfile.close()

	#get current playing team
	curr_team=line.split(',')[3].rstrip('\n')

	if balls=="1" and int(overs)%5!=0:
		log.write("Current score for "+curr_team+":"+str(runs)+"-"+str(wicks)+"\n"+bats1+" : "+bats1run+"\n"+bats2+" : "+bats2run+"\nOvers: "+overs+"\n")
		tweet("Current score for "+curr_team+":"+str(runs)+"-"+str(wicks)+"\n"+bats1+" : "+bats1run+"\n"+bats2+" : "+bats2run+"\nOvers: "+overs)
		time.sleep(1)
		print "Current score for "+curr_team+":"+str(runs)+"-"+str(wicks)+"\n"+bats1+" : "+bats1run+"\n"+bats2+" : "+bats2run+"\nOvers: "+overs


	# when innings break occurs
	if line.split(',')[1]=="2" and inn1graph==0:
		target=str(int(runs)+1)
		curr_bmen[0][0]=""
		curr_bmen[1][0]=""
		curr_bmen[0][1]="0"
		curr_bmen[1][1]="0"


		print "End of 1st innings. Target set for "+curr_team+":"+target
		tweet("End of 1st innings. Target set for "+curr_team+":"+target)
		time.sleep(5)
		log.write("End of 1st innings. Target set for "+curr_team+":"+target+"\n")
		os.system('echo "innbreak" | /home/kafka/Downloads/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic TutorialTopic > /dev/null')
		time.sleep(10)
		#print target
		if inn1graph==0:
			#plotting the 1st innings graph
			graphfile=open('/home/mausam/predscores.txt','r')
			x=[]
			y=[]
			for gline in graphfile:
				x.append(int(gline.split(',')[0])-1)
				y.append(int(gline.split(',')[1]))
			final_score=[]
			for item in x:
				final_score.append(int(runs))
			axes = plt.gca()
			axes.set_xlim([0,9])
			axes.set_ylim([0,400])
			plt.plot(x,y)
			plt.plot(x,final_score)
			plt.ylabel('Predicted score')
			plt.xlabel('After # segments')
			plt.savefig('/home/mausam/Graphs/graph1.png')
			plt.gcf().clear()
		inn1graph=1
		runs=0
		wicks=0

	# send continuous data to flink
	if curr_team==team1:
		pass
	elif curr_team==team2:
		team2=team1
		team1=curr_team
	kafkaargs="overs:"+overs+",balls:"+balls+",team1:"+team1+",team2:"+team2+",runs:"+str(runs)+",wickets:"+str(wicks)+",bats1:"+bats1clusID+",bats2:"+bats2clusID+",venue cluster:"+venue_clus+",target:"+str(target)
	#print kafkaargs+"\n"
	log.write(kafkaargs+"\n")
	os.system('echo "'+kafkaargs+'" | /home/kafka/Downloads/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic TutorialTopic > /dev/null')
	time.sleep(1)
	f.write(kafkaargs+"\n")

	


	# get current batsmen runs	
	if curr_bmen[0][0]=="" and curr_bmen[1][0]=="":
		curr_bmen[0][0]=bats1
		curr_bmen[0][1]=str(int(line.split(',')[7])+int(line.split(',')[8]))
		curr_bmen[1][0]=bats2
		curr_bmen[1][1]="0"
	elif curr_bmen[0][0]=="None":
		if bats1==curr_bmen[1][0]:
			curr_bmen[0][0]=bats2
			curr_bmen[1][1]=str(int(curr_bmen[1][1])+int(line.split(',')[7])+int(line.split(',')[8]))
		else:
			curr_bmen[0][0]=bats1
			curr_bmen[0][1]=str(int(line.split(',')[7])+int(line.split(',')[8]))
		'''print "in first"
		print curr_bmen[0][0],":",curr_bmen[0][1]
		print curr_bmen[1][0],":",curr_bmen[1][1]'''
	elif curr_bmen[1][0]=="None":
		if bats1==curr_bmen[0][0]:
			curr_bmen[1][0]=bats2
			curr_bmen[0][1]=str(int(curr_bmen[0][1])+int(line.split(',')[7])+int(line.split(',')[8]))
		else:
			curr_bmen[1][0]=bats1
			curr_bmen[1][1]=str(int(line.split(',')[7])+int(line.split(',')[8]))
		'''print "in second"
		print curr_bmen[0][0],":",curr_bmen[0][1]
		print curr_bmen[1][0],":",curr_bmen[1][1]'''
	else:
		if bats1==curr_bmen[0][0]:
			curr_bmen[0][1]=str(int(curr_bmen[0][1])+int(line.split(',')[7])+int(line.split(',')[8]))
		else:
			curr_bmen[1][1]=str(int(curr_bmen[1][1])+int(line.split(',')[7])+int(line.split(',')[8]))
	if bats1==curr_bmen[0][0]:
		bats1run=curr_bmen[0][1]
		bats2run=curr_bmen[1][1]
	else:
		bats1run=curr_bmen[1][1]
		bats2run=curr_bmen[0][1]	

	'''print 'Batsman 1: ',bats1,' scored: ',bats1run
	print 'Batsman 2: ',bats2,' scored: ',bats2run'''

	
	if int(bats1run)>=50 and int(bats1run)<56:			#tweet when 50s or 100s for batsmen
		if bats1 not in prev_half:		
			print bats1," scored a half century!"
			tweet(bats1+" scored a half century! ")
			time.sleep(1)
			log.write(bats1+" scored a half century!\n")
			prev_half.append(bats1)
	if int(bats1run)>=100 and int(bats1run)<106:
		if bats1 not in prev_full:
			print bats1," scored a century!!"
			tweet(bats1+" scored a century!! ")
			time.sleep(1)
			log.write(bats1+" scored a century!\n")
			prev_full.append(bats1)
	if int(bats2run)>=50 and int(bats2run)<56:
		if bats2 not in prev_half:
			print bats2," scored a half century!"
			tweet(bats2+" scored a half century! ")
			time.sleep(1)
			log.write(bats2+" scored a half century!\n")
			prev_half.append(bats2)
	if int(bats2run)>=100 and int(bats2run)<106:
		if bats2 not in prev_full:
			print bats2," scored a century!!"
			tweet(bats2+" scored a century!! ")
			time.sleep(1)
			log.write(bats2+" scored a century!\n")
			prev_full.append(bats2)
	#print bats1," score:",int(bats1run)
	log.write(bats1+" score:"+bats1run+"\n")
	#print bats2," score:",int(bats2run)
	log.write(bats2+" score:"+bats2run+"\n")

	# get current wickets from curr_data
	if line.split(',')[10].rstrip('\n')!='""':
		wicks=wicks+1
		print "Wicket lost: "+line.split(',')[10].rstrip('\n')+"\nTaken out by: "+line.split(',')[6]+"\nCurrent score for "+curr_team+":"+str(runs)+"-"+str(wicks)+"\nOvers: "+overs+"."+balls
		log.write("Wicket lost: "+line.split(',')[10].rstrip('\n')+"\nTaken out by: "+line.split(',')[6]+"\nCurrent score for "+curr_team+":"+str(runs)+"-"+str(wicks)+"\nOvers: "+overs+"."+balls+"\n")
		tweet("Wicket lost: "+line.split(',')[10].rstrip('\n')+"\nTaken out by: "+line.split(',')[6]+"\nCurrent score for "+curr_team+":"+str(runs)+"-"+str(wicks)+"\nOvers: "+overs+"."+balls)
		time.sleep(1)
		if curr_bmen[0][0]==line.split(',')[10].rstrip('\n'):
			curr_bmen[0][0]="None"
			curr_bmen[0][1]="0"
		else:
			curr_bmen[1][0]="None"
			curr_bmen[1][1]="0"



	runs=runs+int(line.split(',')[7])+int(line.split(',')[8])
	prev_line=line
	
	


time.sleep(1)
winner=linecache.getline(match_file_path,19).split(',')[2].rstrip('\n')
print winner,' won the match.'
print "Final score for "+curr_team+": "+str(runs)
log.write(winner+' won the match.\n')
log.write("Final score: "+str(runs)+"\n")
tweet(winner+' won the match. Final score: '+str(runs))
time.sleep(3)
os.system('echo "done" | /home/kafka/Downloads/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic TutorialTopic > /dev/null')
time.sleep(10)
#plotting the 2nd innings graph
graphfile=open('/home/mausam/predscores.txt','r')
x=[]
y=[]
for line in graphfile:
	x.append(int(line.split(',')[0])-1)
	y.append(int(line.split(',')[1]))
final_score=[]
final_target=[]
for item in x:
	final_score.append(int(runs))
	final_target.append(int(target))
axes = plt.gca()
axes.set_xlim([0,9])
axes.set_ylim([0,400])
plt.plot(x,y)
plt.plot(x,final_score)
plt.plot(x,final_target)
plt.ylabel('Predicted score')
plt.xlabel('After # segments')
plt.savefig('/home/mausam/Graphs/graph2.png')
f.close()
log.close()
