
%include 'G:\SASDATA\icooke\id_pwd.txt';




proc fontreg mode=add;
fontpath '!SYSTEMROOT\fonts';
run;


proc datasets lib=work kill nolist memtype=data;
quit;


%let fm_sales_goal = 3397861;
%let ytd_sales_goal = 46663150;


%let fm_sales_goal_1 = %eval(&fm_sales_goal/2);
%let fm_sales_goal_2 = %sysevalf(&fm_sales_goal*1.5);

%let ytd_sales_goal_1 = %eval(&ytd_sales_goal/2);
%let ytd_sales_goal_2 = %sysevalf(&ytd_sales_goal*1.5);




/*	Set up calendar	*/

%let day=%sysfunc(intnx(day,%sysfunc(today()),-20));


%let dayq = %str(%')%sysfunc(putn(&day,date9.))%str(%');
%put &dayq;



/*	Find paramters for the MONTH in CIDB	*/


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table calendar as
   select * from connection to netezza
   (

   	with fiscal_yr as (

		select fiscal_yr,
				min(day_dt)::date as first_dt_yr

			from cidb_prd..days

			where fiscal_yr in (select distinct fiscal_yr from cidb_prd..days where day_dt = &dayq)
			group by fiscal_yr
			)

		select a.fiscal_yr,
		a.fiscal_mo,
		a.fiscal_mo_name,
		a.fiscal_mo_name_abbr,
		substr(fiscal_mo,5,2) as month,
		min(day_dt)::date as dt1,
		max(day_dt)::date as dt2,
		b.first_dt_yr

		from cidb_prd..days a, fiscal_yr b

		where a.fiscal_yr = b.fiscal_yr
		and fiscal_mo in (select distinct fiscal_mo from cidb_prd..days where day_dt = &dayq)

		group by a.fiscal_yr,
				a.fiscal_mo,
				a.fiscal_mo_name,
				a.fiscal_mo_name_abbr,
				b.first_dt_yr

   );
   disconnect from netezza;
quit;




data calendar;
	length dt1q dt2q first_dt_yrq $ 11 day1 day2 $ 2;
	set calendar;
	month1 = put(dt1,MONNAME3.);
	month2 = put(dt2,MONNAME3.);
	day1 = tranwrd(put(day(dt1),2.), " ", "0");
	day2 = tranwrd(put(day(dt2),2.), " ", "0");
	dt1q = quote(strip(put(dt1,DATE10.)), "'");
	dt2q = quote(strip(put(dt2,DATE10.)), "'");
	first_dt_yrq = quote(strip(put(first_dt_yr,DATE10.)), "'");
run;


proc sql noprint;
	select fiscal_yr,
			fiscal_mo,
			fiscal_mo_name,
			fiscal_mo_name_abbr,
			month,
			month1,
			month2,
			day1,
			day2,
			dt1q,
			dt2q,
			first_dt_yrq

		into :fiscal_yr,
			:fiscal_mo,
			:fiscal_mo_name,
			:fiscal_mo_name_abbr,
			:month,
			:month1,
			:month2,
			:day1,
			:day2,
			:dt1q,
			:dt2q,
			:first_dt_yrq

	from calendar
	;
quit;





PROC IMPORT OUT= em_summary_list_import DATAFILE= "G:\SASDATA\ICooke\Campaign Reporting\EM campaign results summary list.xlsx" 
            DBMS=xlsx REPLACE;
     SHEET="Sheet1"; 
     GETNAMES=NO;
	 datarow=3;
RUN;


*	columns that had missing values have a text . after importing from excel.
*	multiplying those columns by 1 turns them back into missing values 
*	which is necessary for formating;

data em_summary_list_TY ;
	set em_summary_list_import (keep=A B C D E G H J K L O);

	where A >= &first_dt_yrq.d
		and A <= &dt2q.d;

	incr_sales_cust = G * 1;
	incr_txns= J * 1;
	incr_margin = K * 1;
	incr_sales = L * 1;
	incr_sales_dotcom = O * 1;

	format A MMDDYY8.;
	format D E percent10.1;

	format incr_sales_cust dollar12.2;
	format H incr_txns comma12.0;
	format incr_margin incr_sales incr_sales_dotcom dollar12.;
	drop G J K L O;
	rename H = qty
			A = date
			B = campaign
			C = campaign_cd
			D = open_rate
			E = cto_rate
;
run;




proc sql;
	create table ytd_incr_sales as

	select sum(incr_sales) as ytd

	from em_summary_list_TY

	;
quit;

proc sql noprint;
	select round(ytd, 100000) into :ytd_incr

	from ytd_incr_sales;
quit;


data campaigns;
	set em_summary_list_TY;

where date >= &dt1q.d
	and date <= &dt2q.d;

campaign_cdq = quote(trim(campaign_cd), "'");

;
run;


proc sql noprint;
	select distinct campaign_cdq into :campaign_cd_list separated by ","

	from campaigns
	;
quit;


/*	combine US & CA campaigns for final output	*/


data campaigns_2;
	set campaigns (keep=date campaign open_rate cto_rate qty incr_txns incr_margin incr_sales incr_sales_dotcom) ;
	rownum + 1;
	opens = qty*open_rate;
	clicks = qty*open_rate*cto_rate;
	if substr(campaign,length(campaign)-1) in ('US','CA') then campaign_trunc = substr(campaign,1,length(campaign)-2);
	else campaign_trunc = campaign;
run;

proc sql;
	create table campaigns_3 as
	select date,
			campaign_trunc,
			sum(qty) as ttl_qty format=comma10.,
			sum(incr_txns) as ttl_incr_txns format=comma10.,
			sum(incr_margin) as ttl_incr_margin format=dollar10.,
			sum(incr_sales) as ttl_incr_sales format=dollar10.,
			sum(incr_sales)/sum(qty) as incr_sales_cust format=dollar10.2,
			sum(incr_sales_dotcom) as ttl_incr_sales_dotcom format=dollar10.,
			sum(opens)/sum(qty) as open_rate format=percent8.1,
			sum(clicks)/sum(opens) as cto_rate format=percent8.1

	from campaigns_2

	group by date,
			campaign_trunc
	;
quit;


proc sql;
	create table weekly_incr_sales as
	select date,
			sum(ttl_incr_sales) as sales
		from campaigns_3

		group by date

		order by date
		;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table weekly_incr_sales_2 as
   select dt format=mmddyy.,
			fiscal_wk,
			fiscal_mo

		from connection to netezza
   (
   select date(day_dt) as dt,
   fiscal_wk,
   fiscal_mo

   from cidb_prd..days

   where fiscal_mo = &fiscal_mo

   group by day_dt,
   		fiscal_wk,
		fiscal_mo

	order by day_dt

   );
   disconnect from netezza;
quit;


proc sql;
	create table weekly_incr_sales_3 as
	
	select b.fiscal_wk,
			sum(a.sales) as week_incr_sales


	from weekly_incr_sales a, weekly_incr_sales_2 b

	where a.date = b.dt

	group by b.fiscal_wk

	order by b.fiscal_wk
	;
quit;





proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table total_sends as
   select * from connection to netezza
   (
   select count(*) as total_sends

   from cidb_usr..et_sendjobs a, cidb_usr..et_sent b

   where a.sendid = b.sendid
	and date_trunc('days',b.eventdate) between &dt1q and &dt2q
	and a.type is null

   );
   disconnect from netezza;
quit;



proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table total_opens as
   select * from connection to netezza
   (
   select count(*) as total_opens

   from cidb_usr..et_sendjobs a, cidb_usr..et_opens b

   where a.sendid = b.sendid
	and date_trunc('days',b.eventdate) between &dt1q and &dt2q
	and a.type is null

   );
   disconnect from netezza;
quit;

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table clicks_total as
   select * from connection to netezza
   (
   select count(*) as total_clicks

   from cidb_usr..et_sendjobs a, cidb_usr..et_clicks b

   where a.sendid = b.sendid
	and date_trunc('days',b.eventdate) between &dt1q and &dt2q
	and a.type is null

   );
   disconnect from netezza;
quit;

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table clicks_website as
   select * from connection to netezza
   (
   select count(*) as website_clicks

   from cidb_usr..et_sendjobs a, cidb_usr..et_clicks b

   where a.sendid = b.sendid
	and date_trunc('days',b.eventdate) between &dt1q and &dt2q
	and a.type is null
   and (lower(url) like '%.petsmart.c%'
   		or lower(url) like '%.petperks.c%')

   );
   disconnect from netezza;
quit;

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table clicks_social as
   select * from connection to netezza
   (
   select count(*) as social_clicks

   from cidb_usr..et_sendjobs a, cidb_usr..et_clicks b

   where a.sendid = b.sendid
	and date_trunc('days',b.eventdate) between &dt1q and &dt2q
	and a.type is null

	and (lower(url) like '%instagram.com%'
		or lower(url) like '%facebook.com%'
		or lower(url) like '%youtube.com%'
		or lower(url) like '%plus.google.com%'
		or lower(url) like '%twitter.com%'
		or lower(url) like '%pinterest.com%')

   );
   disconnect from netezza;
quit;

data em_engagement_cnts;
	merge total_sends total_opens clicks_total clicks_website clicks_social;

	format _ALL_ comma12.;
run;

proc sql noprint;
	select total_sends,
			total_opens,
			total_clicks,
			website_clicks,
			social_clicks

			into :ttl_sends, :ttl_opens, :ttl_clicks, :site_clicks, :soc_clicks

	from em_engagement_cnts
	;
quit;








proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
   	create table db_email_metrics as

	select a.sendid,
			b.batchid,
			b.subscriberkey,
			a.campaign_cd,
			date_trunc('days',b.eventdate) as dt,
			date_part('days',b.eventdate) as day,
			date_part('months',b.eventdate) as month,
			0 as open,
			0 as click,
			0 as bounce
		
		from cidb_usr..et_sendjobs a, cidb_usr..et_sent b

		where a.sendid = b.sendid
		and date_trunc('days',b.eventdate) between &dt1q and &dt2q
		and a.type is null

		group by a.sendid,
			b.batchid,
			b.subscriberkey,
			a.campaign_cd,
			dt,
			day,
			month

;
   )
   by netezza;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (

		update db_email_metrics a
		set open = 1
		where exists (select distinct b.subscriberkey
						from cidb_usr..et_opens b
						where a.sendid = b.sendid
						and a.batchid = b.batchid
						and a.subscriberkey = b.subscriberkey
						and b.eventdate between a.dt and (a.dt + 8)
						)
		;
   )
   by netezza;
quit;



proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (

		update db_email_metrics a
		set click = 1
		where exists (select distinct b.subscriberkey
						from cidb_usr..et_clicks b
						where a.sendid = b.sendid
						and a.batchid = b.batchid
						and a.subscriberkey = b.subscriberkey
						and b.eventdate between a.dt and (a.dt + 8)
						)
		;
   )
   by netezza;
quit;



proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (

		update db_email_metrics a
		set bounce = 1
		where exists (select distinct b.subscriberkey
						from cidb_usr..et_bounces b
						where a.sendid = b.sendid
						and a.batchid = b.batchid
						and a.subscriberkey = b.subscriberkey
						and b.eventdate between a.dt and (a.dt + 8)
						)
		;
   )
   by netezza;
quit;



proc sql;
connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
create table send_metrics as
   select * from connection to netezza
   (
	select dt,
			month||'/'||day as dt2,
			campaign_cd,
			count(subscriberkey) as sends,
			sum(open) as opens,
			sum(click) as clicks,
			sum(bounce) as bounces,
			sends - bounces as delivered,
			opens/delivered as open_rate,
			clicks/opens as cto_rate
		
		from db_email_metrics
			where campaign_cd like 'C00001%'
		
		group by dt,
			dt2,
			campaign_cd
		order by dt,
			campaign_cd
   );
   disconnect from netezza;
quit;


proc sql;
	create table send_metrics_2 as
	select a.dt,
	compress(a.dt2)||" "||b.campaign as camp,
	a.campaign_cd,
	b.campaign,
	a.sends,
	a.opens,
	a.clicks,
	a.bounces,
	a.delivered,
	a.open_rate format=percent8.2,
	a.cto_rate format=percent8.2,
	b.incr_sales

	from send_metrics a, em_summary_list_TY b
	where a.campaign_cd = b.campaign_cd

	group by a.dt,
	camp,
	a.campaign_cd,
	a.sends,
	a.opens,
	a.clicks,
	a.bounces,
	a.delivered,
	a.open_rate,
	a.cto_rate,
	b.incr_sales

	order by a.dt,
	a.dt2,
	a.campaign_cd
	;
quit;





proc sql;
create table ttl_email_metrics as

select sum(opens)/sum(delivered) as open_rate,
	sum(clicks)/sum(opens) as cto_rate

from send_metrics_2

;
quit;


proc sql noprint;
	select open_rate,
			cto_rate

			into :open_rate,
				:cto_rate

	from ttl_email_metrics
;
quit;



/*	Combine US & CA cmapaigns	
	Since Feb 01 2016 US and CA campaigns are always split.
	Need to combine them to fit on 1 page for output
*/


data send_metrics_3;
	set send_metrics_2 (keep=camp sends opens clicks delivered incr_sales) ;
	rownum + 1;
	if substr(camp,length(camp)-1) in ('US','CA') then camp_trunc = substr(camp,1,length(camp)-2);
	else camp_trunc = camp;
run;

proc sql;
	create table send_metrics_4 as
	select camp_trunc,
			sum(sends) as sent,
			sum(opens)/sum(delivered) as open_rate format=percent8.2,
			sum(clicks)/sum(opens) as cto_rate format=percent8.2,
			sum(incr_sales) as ttl_incr_sales format=dollar12.2

	from send_metrics_3

	group by camp_trunc
	;
quit;

**keep only top 10 campaigns for bubble chart;
proc sort data=send_metrics_4;
	by DESCENDING ttl_incr_sales;
run;

data send_metrics_5;
	set send_metrics_4;
	x+1;
	if x<=10;
run;


proc sql;
	create table week_summary as

	select 	sum(qty) as total_customers,
			sum(incr_txns) as incremental_trips,
			sum(incr_margin) as incremental_margin,
			sum(incr_sales) as incremental_sales

	from campaigns

	;
quit;


data week_summarya;
	set week_summary;
	incremental_SPC = incremental_sales/total_customers;

	format incremental_trips comma10.;
	format incremental_margin incremental_sales dollar10.;
	format incremental_SPC dollar10.2;
run;




ods path(prepend) work.template(update);

/*

*	bubble chart;


goptions reset=all border;
	
proc template;
  define statgraph bubbles;
    begingraph;
      entrytitle 'Email Metrics: Top 10 Campaigns (Incr Sales)';
      layout overlay /
          xaxisopts = (label='Open rate' type=linear)
          yaxisopts = (label='Click-to-Open Rate');
        bubbleplot x=open_rate y=CTO_rate size=sent / name='bubble'
          group=camp_trunc datatransparency=0.5;
		  referenceline x=.16 / curvelabellocation=inside curvelabel="Goal=16%" DATATRANSPARENCY=.5 CURVELABELPOSITION=MIN;
		  referenceline y=.08 / curvelabellocation=inside curvelabel="Goal=8%" DATATRANSPARENCY=.5 CURVELABELPOSITION=MIN;
		  discretelegend 'bubble' / border=false location=outside halign=center
 			valign=bottom; 

      endlayout;
    endgraph;
  end;
*/



/*	new YTD sales vs forecast
  	still need to automate this
*/

proc sql noprint;
	select round(incremental_sales, 100000) into :incr_sales_week

	from week_summary
	;
quit;





*	dials;

proc format;
picture sales 0='9' (prefix='$')
 1-999='0,000' (prefix='$ ')
 1000- 999999='0,000.0' K (mult=.01 prefix='$ ')
 1000000-999999999='0,000.0' M (mult=.00001 prefix='$ ')
 10000000000-high='0,000.0' B (mult=.00000001 prefix='$ ')
 ;
run; 


goptions reset=all device=javaimg vsize=1.1in hsize=2.3in;

ods graphics / reset=index;
ods html gpath="G:\SASDATA\icooke\updates\EM Monthly Dashboard\images";

proc gkpi mode=basic;
hbullet actual=&incr_sales_week bounds=(0 &fm_sales_goal_1 &fm_sales_goal &fm_sales_goal_2) /
   format="sales."
   afont=(f="roboto" height=1cm)
   bfont=(f="roboto light" height=.4cm )
   name="sales_goal_month"  
   target=&fm_sales_goal;
run; quit;


proc gkpi mode=basic;
hbullet actual=&ytd_incr bounds=(0 &ytd_sales_goal_1 &ytd_sales_goal &ytd_sales_goal_2) /
   format="sales."
   afont=(f="roboto" height=1cm)
   bfont=(f="roboto light" height=.4cm )
   name="sales_goal_ytd"  
   target=&ytd_sales_goal;
run; quit;


goptions reset=all device=javaimg vsize=2.5in hsize=3.2in;

proc gkpi mode=basic;
   dial actual=&open_rate bounds=(.06 .11 .16 .20 .25) /   target=.16
   afont=(f="roboto" height=.8cm) 
   bfont=(f="roboto light" height=.4cm)
   colors=(cxD06959 cxF1DC63 cxBDCD5F cx84AF5B)
   format="percent8.1"
   name="open_rate";
run;
quit;

proc gkpi mode=basic;
   dial actual=&cto_rate bounds=(.02 .05 .08 .10 .13) /   target=.08
   afont=(f="roboto" height=.8cm) 
   bfont=(f="roboto light" height=.4cm)
   colors=(cxD06959 cxF1DC63 cxBDCD5F cx84AF5B)
   format="percent8.1"
   name="cto_rate";
run;
quit;

ods html close;




* weekly sales bar chart format;

proc format;
picture chart_sales low-999='0,000' (prefix='$')
 1000- 999999='000' K (mult=.001 prefix='$ ')
 1000000-999999999='0,000.0' M (mult=.00001 prefix='$ ')
 10000000000-high='0,000.0' B (mult=.00000001 prefix='$ ')
 ;
run; 




* output PDF;

proc template; 
 define style styles.emdashboard; 
 parent = styles.sasweb; 

 style header from header / 
	font_face=ubuntu
	font_size = 10pt 
	just = center 
	vjust = center
	cellwidth=1.1in
	cellheight=.5in
	bordercolor=black
	background=cx606060
	; 

 style data from data /
 	font_face='roboto light'
 	font_size = 10pt
	just = c
	vjust = center
	font_face=arial
	cellheight=.3in
	background=white 
	bordercolor=black;

  end; 
run; quit; 




*	Absolute layout;

title;

ods listing close;
ods noresults;
options nodate nonumber orientation=landscape bottommargin=.1in;
ods pdf notoc file="G:\SASDATA\icooke\updates\EM Monthly Dashboard\Fiscal Month &fiscal_mo_name_abbr Email Campaign Dashboard.pdf" color=yes
	style=emdashboard;

ODS escapechar="^"; 

title j=l font= 'Times New Roman' height=25pt color=cx003CA8 'Email Campaign Dashboard';

ods layout absolute;


ods region width=10.5in height=1in;
title j=l font= 'ubuntu' height=18pt color=white bcolor=cx003CA8 "Fiscal Month &fiscal_mo_name.: &month1 &day1 - &month2 &day2";
ods text=' ';



**placeholder;



ods region width=1.7in height=1in x=6.3in y=3.0in;
ods PDF text= "^S={just=c font_weight=bold font_size=10pt font_face=Arial color=black}&fiscal_mo_name_abbr. Incr. Sales";
ods PDF text="^S={preimage='G:\SASDATA\icooke\updates\EM Monthly Dashboard\images\sales_goal_month.png?height=70%&width=100%'}" ;


ods region width=1.7in height=1in x=8.5in y=3.0in;
ods PDF text= "^S={just=c font_weight=bold font_size=10pt font_face=Arial color=black}YTD Incremental Sales";
ods PDF text="^S={preimage='G:\SASDATA\icooke\updates\EM Monthly Dashboard\images\sales_goal_ytd.png?height=70%&width=100%'}" ;



ods region width=2.2in height=2in y=0.5in;
ods PDF text= "^S={just=c font_weight=bold font_size=8pt font_face=Arial color=black}&fiscal_mo_name_abbr. Open rate";
ods PDF text="^S={preimage='G:\SASDATA\icooke\updates\EM Monthly Dashboard\images\open_rate.png?height=80%&width=100%'}" ;

ods region width=2.2in height=2in x=1.85in y=0.5in;
ods PDF text= "^S={just=c font_weight=bold font_size=8pt font_face=Arial color=black}&fiscal_mo_name_abbr. CTO rate";
ods PDF text="^S={preimage='G:\SASDATA\icooke\updates\EM Monthly Dashboard\images\cto_rate.png?height=80%&width=100%'}" ;



ods region width=3.9in height=2in x=4in y=0.5in;

proc report data=em_engagement_cnts
	style(report) = {cellspacing=0}
	style(header) =	{cellwidth=1.5in cellheight=.3in}
	style(column) = {font_face='roboto light' cellheight=.5in font_size = 18pt background=cxEDEDED };

	column total_sends total_opens;

	define total_sends			/display 'Total Sends';
	define total_opens			/display 'Total Opens'  style={cellwidth=1.4in};

run;

ods text=' ^n';


proc report data=em_engagement_cnts
	style(report) = {cellspacing=0}
	style(header) =	{cellwidth=1.5in cellheight=.3in}
	style(column) = {font_face='roboto light' cellheight=.5in font_size = 18pt background=cxEDEDED};

	column total_clicks website_clicks;

	define total_clicks			/display 'Total Clicks'   style={cellwidth=1.4in};
	define website_clicks		/display 'Website Clicks' ;

run;




ods region width=6.5in height=1.5in y=2.5in;

title j=l font= 'ubuntu' height=15pt color=white bcolor=cx003CA8 'Campaign Summary Results';


proc report data=week_summarya
	style(report) ={cellspacing=0}
	style(column) = {font_face='roboto light' cellheight=.5in font_size = 14pt background=cxEDEDED};

	column incremental_trips incremental_margin incremental_sales incremental_SPC;

	define incremental_trips		/display 'Incremental/Trxns' style={cellwidth=200};
	define incremental_margin		/display 'Incremental/Margin' style={cellwidth=200};
	define incremental_sales		/display 'Incremental/Sales' style={cellwidth=200};
	define incremental_SPC			/display 'Avg Incr. Sales/per Customer' style={cellwidth=250}; 


run;






ods region width=3.5in height=5.5in x=7.0in y=0.3in ;

ods graphics on / border=off;

*style=[backgroundcolor=yellow]
*ods text='abc';
*	NOT SURE WHY THIS CHART IS CAUSING ALL FURTHER TITLES TO DISAPPEAR;

proc sgplot data=weekly_incr_sales_3;
   vbar fiscal_wk / response=week_incr_sales barwidth=.5 fillattrs=(color=cx84AF5B) nooutline;
   xaxis label='Fiscal Week';
   yaxis label='Incremental Sales' TICKVALUEFORMAT=chart_sales.;
   title;
run;


ods layout end;


ods layout gridded y=4in;

ods region;


title2 j=l font= 'ubuntu' height=15pt color=white bcolor=cx003CA8 'Campaign Detail';


proc report data=campaigns_3
	style(report) ={rules=none cellspacing=0}
	style(column) = {font_face='roboto light' cellheight=.18in font_size = 10pt background=cxEDEDED};

	column date campaign_trunc ttl_qty open_rate cto_rate ttl_incr_txns ttl_incr_margin ttl_incr_sales incr_sales_cust ttl_incr_sales_dotcom;

	define date							/display 'Date' style={cellwidth=130};
	define campaign_trunc				/display 'Campaign' style={cellwidth=310};
	define ttl_qty						/display 'Size' style={cellwidth=150};
	define open_rate					/display 'Open Rate' style={cellwidth=120};
	define cto_rate						/display 'CTO Rate' style={cellwidth=120}; 
	define ttl_incr_txns				/display 'Incremental/Transactions' style={cellwidth=140}; 
	define ttl_incr_margin				/display 'Incremental/Margin' style={cellwidth=150}; 
	define ttl_incr_sales				/display 'Incremental/Sales - Ttl' style={cellwidth=150}; 
	define incr_sales_cust				/display 'Incr. Sales/per Customer' style={cellwidth=150}; 
	define ttl_incr_sales_dotcom		/display 'Incremental/Sales .com' style={cellwidth=150};


run;

ods layout end;



ods pdf close;




proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (

		drop table DB_EMAIL_METRICS
		;
   )
   by netezza;
quit;


ODS HTML;

proc sgplot data=weekly_incr_sales_3;
   vbar fiscal_wk / response=week_incr_sales barwidth=.5 fillattrs=(color=cx84AF5B) nooutline;
   xaxis label='Fiscal Week';
   yaxis label='Incremental Sales' TICKVALUEFORMAT=chart_sales.;
   title;
run;

ODS HTML close;
