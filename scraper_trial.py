from lxml import html
import requests
import sys
import time
import os
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def tweet(post):
	mod_post=post.replace(" ","%20")
	mod_post=mod_post.replace("\r","%20")
	mod_post=mod_post.replace("\t","%20")
	mod_post=mod_post.replace("\n","%0A")
	#print mod_post
	os.system('curl -i http://localhost:5000/social_media/handler/'+mod_post+'/1')

now = datetime.datetime.now()
log=open('/home/mausam/crawlerlog.txt','a')
teamindex={'INDIA':'0','SRI LANKA':'1','NZ':'2','AUSTRALIA':'3','PAKISTAN':'4','BAN':'5','ENGLAND':'6','WEST INDIES':'7','SOUTH AFRICA':'8'}
teamanno={'India':'INDIA','Sri Lanka':'SRI LANKA','New Zealand':'NZ','Australia':'AUSTRALIA','Pakistan':'PAKISTAN','Bangladesh':'BAN','England':'ENGLAND','West Indies':'WEST INDIES','South Africa':'SOUTH AFRICA'}
rn=0
wn=0
rnminus1=0
wnminus1=0
prev_curr_data=""
prev_overs=0
url=str(sys.argv[1])
page = requests.get(url)
tree = html.fromstring(page.content)
matchinfo=tree.xpath('//div[@class="match-information-strip"]/text()')[0]
print matchinfo
tweet(matchinfo)
time.sleep(5)
log.write(matchinfo+"\n")
if len(matchinfo.split(','))>=2 and len(matchinfo.split(',')[1].split(' at '))>=2:
	venue=matchinfo.split(',')[1].split(' at ')[1]
else:
	venue=matchinfo.split(' at ')[1].split(',')[0]
print venue
team1=tree.xpath('//div[@class="team-1-name"]/text()')[0]
team2=tree.xpath('//div[@class="team-2-name"]/text()')[0]
venue_clus="1"
vfile=open('/home/mausam/venues.txt','r')
for vline in vfile:
	if vline.split(',')[0]==venue:
		venue_clus=vline.split(',')[1].rstrip('\n')
vfile.close()
target="0"
prev_seg=0
toss=0
prev_bmen=["",""]
prev_wicks=0
prev_half=["None"]
prev_full=["None"]
inn1graph=0
filename=str(now.year)+"-"+str(now.month)+"-"+str(now.day)+"-"+team1+"-"+team2+".txt"
#f=open('/home/mausam/newmatch/'+filename,'a')
while True:
	time.sleep(2)
	# get webpage contents from espncricinfo.com
	try:
		page = requests.get(url)
	except requests.exceptions.RequestException as e:
		print 'Request Exception... Trying again...'
		log.write('Request Exception... Trying again...\n')
		continue
	tree = html.fromstring(page.content)
	# get inning requirements
	innreqr=tree.xpath('//div[@class="innings-requirement"]/text()')
	# get current info for match
	curr_data=tree.xpath('//head/title/text()')[0]
	if curr_data==prev_curr_data:
		continue
	prev_curr_data=curr_data
	print curr_data
	team1=tree.xpath('//div[@class="team-1-name"]/text()')[0]
	team2=tree.xpath('//div[@class="team-2-name"]/text()')[0]
	if len(innreqr)==0:
		continue
	print innreqr[0]
	log.write(innreqr[0]+"\n")
	# get current runs from curr_data
	runs=curr_data.split(' ')[1].split('/')[0]
	# get current wickets from curr_data
	if len(curr_data.split(' ')[1].split('/'))==2:
		wicks=curr_data.split(' ')[1].split('/')[1]
	else:
		wicks="10"
	if "won" in innreqr[0].split(' ') and "toss" in innreqr[0].split(' ') and toss==0:
		print innreqr[0]
		tweet(innreqr[0].rstrip('\r').lstrip('\t'))
		time.sleep(1)
		log.write(innreqr[0]+'\n')
		toss=1
	if "won" in innreqr[0].split(' ') and "toss" not in innreqr[0].split(' '):
		print innreqr[0]
		tweet(innreqr[0])
		time.sleep(1)
		log.write(innreqr[0]+'\n')
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
		break
	log.write(curr_data+"\n")
	if len(curr_data.split('('))<2:
		continue
	#get current playing team
	curr_team=curr_data.split(' ')[0]
	# if match abandoned
	if curr_data.find("Match abandoned") != -1:
		print "Match Abandoned!"
		tweet("Match abandoned")
		time.sleep(2)
		log.write("Match abandoned\n")
		os.system('echo "done" | /home/kafka/Downloads/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic TutorialTopic > /dev/null')
		break
	# get current status from curr_data
	if "-" in curr_data.split(' '):
		status=curr_data.split(' - ')[1].split(' | ')[0]
	else:
		status="None"
	
	# when innings break occurs
	if status=="Innings break":
		target=str(int(runs)+1)
		rn=0
		wn=0
		wnminus1=0
		rnminus1=0
		tweet("End of 1st innings. Target set by "+curr_team+":"+target)
		time.sleep(5)
		log.write("End of 1st innings. Target set by "+curr_team+":"+target+"\n")
		print "Innings break. Target: ",target
		os.system('echo "innbreak" | /home/kafka/Downloads/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic TutorialTopic > /dev/null')
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
		continue
	#get overs and balls
	overs=curr_data.split(' ')[2].split('(')[1].split('.')[0]
	balls=curr_data.split(' ')[2].split('(')[1].split('.')[1]
	
	# get current batsmen	
	bats1=curr_data.split(',')[1].split(' ')[:-1]
	bats1run=curr_data.split(',')[1].split(' ')[-1].split('*')[0]
	b1=""
	x=1
	while x<(len(bats1)-1):
		b1=b1+bats1[x]+" "
		x=x+1
	b1=b1+bats1[x]
	bats2run="0"
	if len(curr_data.split(','))==4:
		bats2=curr_data.split(',')[2].split(' ')[:-1]
		bats2run=curr_data.split(',')[2].split(' ')[-1].split('*')[0]
	elif len(curr_data.split(','))<=2:
		time.sleep(1)
		continue
	#elif prev_bmen[1]!="None" and wicks>prev_wicks:
	elif wicks>prev_wicks:
		if len(curr_data.split(','))==4:
				#print curr_data.split(',')[3].split(')')[0].split(' ')[:-1][0]
				#print curr_data.split(',')[3].split(')')[0].split(' ')[:-1][1]
				bowler=curr_data.split(',')[3].split(')')[0].split(' ')[:-1][1]+" "+curr_data.split(',')[3].split(')')[0].split(' ')[:-1][2]
		elif len(curr_data.split(','))==3:
				#print curr_data.split(',')[2].split(')')[0].split(' ')[:-1][0]
				#print curr_data.split(',')[2].split(')')[0].split(' ')[:-1][1]
				bowler=curr_data.split(',')[2].split(')')[0].split(' ')[:-1][1]+" "+curr_data.split(',')[2].split(')')[0].split(' ')[:-1][2]
		if b1==prev_bmen[0]:				# tweet when wicket loss
			print "Wicket lost: "+prev_bmen[1]+"\nTaken out by: "+bowler+"\nCurrent score for "+curr_team+": "+runs+"-"+wicks+"\nOvers:"+overs+"."+balls
			tweet("Wicket lost: "+prev_bmen[1]+"\nTaken out by: "+bowler+"\nCurrent score for "+curr_team+": "+runs+"-"+wicks+"\nOvers:"+overs+"."+balls)
			time.sleep(1)
			log.write("Wicket lost: "+prev_bmen[1]+"\nTaken out by: "+bowler+"\nCurrent score for "+curr_team+": "+runs+"-"+wicks+"\nOvers:"+overs+"."+balls+"\n")
			prev_wicks=wicks
		else:
			print "Wicket lost: "+prev_bmen[0]+"\nTaken out by: "+bowler+"\nCurrent score for "+curr_team+": "+runs+"-"+wicks+"\nOvers:"+overs+"."+balls
			tweet("Wicket lost: "+prev_bmen[0]+"\nTaken out by: "+bowler+"\nCurrent score for "+curr_team+": "+runs+"-"+wicks+"\nOvers:"+overs+"."+balls)
			time.sleep(1)
			log.write("Wicket lost: "+prev_bmen[0]+"\nTaken out by: "+bowler+"\nCurrent score for "+curr_team+": "+runs+"-"+wicks+"\nOvers:"+overs+"."+balls+"\n")
			prev_wicks=wicks
		bats2=[" ","None"]
		#continue
	b2=""
	x=1
	while x<(len(bats2)-1):
		b2=b2+bats2[x]+" "
		x=x+1
	b2=b2+bats2[x]
	prev_bmen[1]=b2
	prev_bmen[0]=b1
	if int(bats1run)>=50 and int(bats1run)<56:			#tweet when 50s or 100s for batsmen
		if b1 not in prev_half:		
			print b1," scored a half century!"
			tweet(b1+" scored a half century! ")
			time.sleep(1)
			log.write(b1+" scored a half century!\n")
			prev_half.append(b1)
	if int(bats1run)>=100 and int(bats1run)<106:
		if b1 not in prev_full:
			print b1," scored a century!!"
			tweet(b1+" scored a century!! ")
			time.sleep(1)
			log.write(b1+" scored a century!\n")
			prev_full.append(b1)
	if int(bats2run)>=50 and int(bats2run)<56:
		if b2 not in prev_half:
			print b2," scored a half century!"
			tweet(b2+" scored a half century! ")
			time.sleep(1)
			log.write(b2+" scored a half century!\n")
			prev_half.append(b2)
	if int(bats2run)>=100 and int(bats2run)<106:
		if b2 not in prev_full:
			print b2," scored a century!!"
			tweet(b2+" scored a century!! ")
			time.sleep(1)
			log.write(b2+" scored a century!\n")
			prev_full.append(b2)
	print b1," score:",int(bats1run)
	log.write(b1+" score:"+bats1run+"\n")
	print b2," score:",int(bats2run)
	log.write(b2+" score:"+bats2run+"\n")
	if balls=="":
		balls="0"
	
	# get batsmen cluster IDs
	bats1clusID="3" 
	bats2clusID="3"
	bfile=open('/home/mausam/batsmen_cluster.csv','r')
	for bline in bfile:
		if bline.split(',')[0]==b1:
			bats1clusID=bline.split(',')[2].rstrip('\n')
		if bline.split(',')[0]==b2:
			bats2clusID=bline.split(',')[2].rstrip('\n')
	bfile.close()
	'''# get current segment
	segment=(int(overs)/5)+1
	if segment==prev_seg:
		continue
	prev_seg=segment'''
	'''# get current team IDs
	if teamanno[team1]==curr_team:
		team2=teamindex[teamanno[team2]]
		team1=teamindex[curr_team]
	else:
		team2=teamindex[teamanno[team1]]
		team1=teamindex[curr_team]'''
	# get current team IDs
	if teamanno[team1]==curr_team:
		pass
	else:
		temp=team1
		team1=team2
		team2=temp
	#process overs other than end of segment
	if int(overs)%5!=0 and (balls=="0" or balls=="1"):
		'''# get batsmen cluster IDs
		bats1clusID="3" 
		bats2clusID="3"
		bfile=open('/home/mausam/batsmen_cluster.csv','r')
		for bline in bfile:
			if bline.split(',')[0]==b1:
				bats1clusID=bline.split(',')[2].rstrip('\n')
			if bline.split(',')[0]==b2:
				bats2clusID=bline.split(',')[2].rstrip('\n')
		bfile.close()
		# get current segment
		segment=(int(overs)/5)+1
		if segment==prev_seg:
			continue
		prev_seg=segment
		# get current team IDs
		if teamanno[team1]==curr_team:
			team2=teamindex[teamanno[team2]]
			team1=teamindex[curr_team]
		else:
			team2=teamindex[teamanno[team1]]
			team1=teamindex[curr_team]
		# get r(n), w(n), r(n-1), w(n-1)
		rnminus1=rnminus1+rn
		wnminus1=wnminus1+wn
		rn=int(runs)-rnminus1
		wn=int(wicks)-wnminus1
		#print segment
		print team1
		print team2
		print curr_team
		print runs
		print wicks
		print b1
		print b2
		print overs
		print balls
		# create input argument for kafka message queue
		kafkaargs="segment:"+str(segment)+",team1:"+team1+",team2:"+team2+",r(n-1):"+str(rnminus1)+",w(n-1):"+str(wnminus1)+",bats1:"+bats1clusID+",bats2:"+bats2clusID+",venue cluster:"+venue_clus+",r(n):"+str(rn)+",w(n):"+str(wn)+",target:"+str(target)
		print kafkaargs
		log.write(kafkaargs+"\n")
		os.system('echo "'+kafkaargs+'" | /home/kafka/Downloads/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic TutorialTopic > /dev/null')
		time.sleep(8)
		#f.write(kafkaargs+"\n")'''
		print 'eoo...'
		print overs
		print prev_overs
		if overs>prev_overs:
			log.write("Current score for "+curr_team+": "+runs+"-"+wicks+"\n"+b1+" : "+bats1run+"\n"+b2+" : "+bats2run+"\nOvers: "+overs+"\n")
			tweet("Current score for "+curr_team+": "+runs+"-"+wicks+"\n"+b1+" : "+bats1run+"\n"+b2+" : "+bats2run+"\nOvers: "+overs)
			time.sleep(1)
			print "Current score for "+curr_team+": "+runs+"-"+wicks+"\n"+b1+" : "+bats1run+"\n"+b2+" : "+bats2run+"\nOvers: "+overs
			time.sleep(8)
			prev_overs=overs
	'''elif balls=="0":
		print "here"
		if overs>prev_overs:
			log.write("Current score for "+curr_team+": "+runs+"-"+wicks+"\n"+b1+" : "+bats1run+"\n"+b2+" : "+bats2run+"\nOvers: "+overs+"\n")
			tweet("Current score for "+curr_team+": "+runs+"-"+wicks+"\n"+b1+" : "+bats1run+"\n"+b2+" : "+bats2run+"\nOvers: "+overs)
			time.sleep(1)
			print "Current score for "+curr_team+": "+runs+"-"+wicks+"\n"+b1+" : "+bats1run+"\n"+b2+" : "+bats2run+"\nOvers: "+overs
			time.sleep(8)
			prev_overs=overs'''

	#send continuous data to flink
	kafkaargs="overs:"+overs+",balls:"+balls+",team1:"+team1+",team2:"+team2+",runs:"+runs+",wickets:"+wicks+",bats1:"+bats1clusID+",bats2:"+bats2clusID+",venue cluster:"+venue_clus+",target:"+str(target)
	print kafkaargs
	log.write(kafkaargs+"\n")
	os.system('echo "'+kafkaargs+'" | /home/kafka/Downloads/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic TutorialTopic > /dev/null')
	time.sleep(1)

#f.close()
log.close()
