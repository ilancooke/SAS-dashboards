/*
	CONTROL + A 
	RUN

	within 21 days of end of month
*/


/*	
	
	Due to SAS technical limitation (google Problem Note 37063), multiple titles cannot be placed along overlapping y axis ranges
	Possible solution: separate regions just for titles and then offset the y axis of the second title
	just enough so it doesn't overlap with the first title's region.  
	These title regions are defined first to make sure they don't get overlapped by the graphs.

*/


%include 'G:\SASDATA\icooke\id_pwd.txt';


options nolabel ls=256 nocenter error=3;
options mlogic mprint symbolgen source source2 MSGLEVEL=I;
options threads cpucount=3 fullstimer;





proc datasets lib=work kill nolist memtype=data;
quit;



proc fontreg mode=add;
fontpath '!SYSTEMROOT\fonts';
run;


/*	Set up calendar	*/

%let day=%sysfunc(intnx(day,%sysfunc(today()),-21));


%let dayq = %str(%')%sysfunc(putn(&day,date9.))%str(%');





/*	Find paramters for the month in CIDB	*/


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table calendar as
   select * from connection to netezza
   (



 		select a.fiscal_yr,
		a.fiscal_mo,
		a.FISCAL_MO_NAME,
		a.FISCAL_MO_NAME_ABBR,
		a.fiscal_mo_nbr,
		min(day_dt)::date as dt1,
		max(day_dt)::date as dt2

		from cidb_prd..days a
		where fiscal_mo in (select distinct fiscal_mo from cidb_prd..days where day_dt = &dayq)

		group by a.fiscal_yr,
				a.fiscal_mo,
				a.FISCAL_MO_NAME,
				a.FISCAL_MO_NAME_ABBR,
				a.fiscal_mo_nbr

   );
   disconnect from netezza;
quit;



data calendar;
	length dt1q dt2q $ 11;
	set calendar;
	fiscal_mo_nameq = quote(strip(fiscal_mo_name), "'");
	fiscal_mo_name_abbrq = quote(strip(fiscal_mo_name_abbr), "'");
	dt1q = quote(strip(put(dt1,DATE10.)), "'");
	dt2q = quote(strip(put(dt2,DATE10.)), "'");
run;


proc sql noprint;
	select dt1q,
			dt2q,
			fiscal_mo,
			fiscal_mo_name,
			fiscal_mo_nameq,
			fiscal_mo_name_abbrq
			

	 into :dt1q, :dt2q, :fiscal_mo, :fiscal_mo_name, :fiscal_mo_nameq, :fiscal_mo_name_abbrq

	from calendar
	;
quit;




/* Get rolling 13 week period  */

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table calendar_13_wks as
   select * from connection to netezza
   (

	select day_dt,
			fiscal_wk,
			weeknum

	from 

	(select distinct cast(day_dt as date) as day_dt,
          ROW_NUMBER() OVER (ORDER BY day_dt desc),
		  fiscal_wk,
		  dense_rank() OVER (ORDER BY fiscal_wk desc) as weeknum
          

   		from cidb_prd..days

   		where fiscal_mo between 201301 and &fiscal_mo
   
   		order by day_dt desc

	) a

	where weeknum <= 13
   );
   disconnect from netezza;
quit;


proc sql noprint;
	select min(fiscal_wk),
			max(fiscal_wk),
			min(day_dt)

			into :hist_start, :hist_end, :hist_start_day

	from calendar_13_wks
	;
quit;


%let hist_start_dayq = %str(%')%sysfunc(putn(&hist_start_day,date9.))%str(%');





/* Get rolling 12 month period  */

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table calendar_12_mos as
   select * from connection to netezza
   (

	select day_dt,
			fiscal_mo,
			monthnum

	from 

	(select distinct cast(day_dt as date) as day_dt,
          ROW_NUMBER() OVER (ORDER BY day_dt desc),
		  fiscal_mo,
		  dense_rank() OVER (ORDER BY fiscal_mo desc) as monthnum
          

   		from cidb_prd..days

   		where fiscal_mo between 201301 and &fiscal_mo
   
   		order by day_dt desc

	) a

	where monthnum <= 12
   );
   disconnect from netezza;
quit;


proc sql noprint;
	select min(fiscal_mo),
			min(day_dt)

		into :hist_start_12, :hist_start_day_12

	from calendar_12_mos
	;
quit;



%let first_dt_yrq = %str(%')%sysfunc(putn(&hist_start_day_12,date9.))%str(%');


/*	END Calendar	*/




/* 	Strategic Segments counts	*/

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
   
   create table ca_db_sol_1 as

   select customer_eid,
          a.store_nbr,
          country_cd,
          count(distinct sales_instance_id) as tranx,
          max(day_dt) as max_dt,
          row_number() over(partition by customer_eid order by tranx desc, max_dt desc, country_cd desc) as cnt
   from CIDB_PRD..sales_trans_sku_usd_vw a,
        cidb_prd..SITE_PROFILE_RPT b
   where customer_master_id > 0 and
         a.STORE_NBR = b.STORE_NBR 
 and fiscal_mo = &fiscal_mo
   group by customer_eid,
            a.store_nbr,
            country_cd
   order by customer_eid,
            tranx desc,
            max_dt desc,
            country_cd desc

   distribute on(customer_eid)
	;

   )
   by netezza;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
      create table ca_db_sol_2 as

   select customer_eid,
          store_nbr,
          country_cd,
          tranx,
          max_dt

   from ca_db_sol_1

   where cnt = 1

   distribute on(customer_eid)
;
   )
   by netezza;
quit;





proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
   update ca_db_sol_2
	set country_cd = 'PR'
	where store_nbr in (select distinct STORE_NBR
	from cidb_prd..SITE_PROFILE_RPT
	where upper(STATE_CD) = 'PR'
	)
	;
   )
   by netezza;
quit;



proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
 		create table ca_db_sol_allcust as
			select a.customer_eid,
			case 
				when upper(b.country) like '%CA%' then 'CA'
				when upper(b.country) like '%US%' then 'US'
				else 'UNK' end as country
			,0 as petperks
			,0 as active_12m
			,0 as em
			,0 as dm
			from cidb_prd..cph_customer_xref a left join CIDB_PRD..cph_customer_address b
			on a.CUSTOMER_EID = b.CUSTOMER_EID
			group by a.customer_eid,
			country

   ;
   )
   by netezza;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_sol_allcust
			set country  = 'PR'
			where customer_eid in (select distinct customer_eid
									from CIDB_PRD..cph_customer_address
			            			where upper(country) = 'USA'
									and upper(STATE) = 'PR'
									)
	;
   )
   by netezza;
quit;

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_sol_allcust
		set country  = 'CA'
		where (customer_eid in (select distinct customer_eid
								from ca_db_sol_2
             					where upper(country_cd) = 'CA'
								)
			or customer_eid in (select distinct customer_eid
								from CIDB_PRD..cph_customer_email
		              			where email_addr like '%.CA' and
		        				primary_email_flg = 'Y' and
		        				active_flg = 'Y'
								)
								)
		and country = 'UNK'
	;
   )
   by netezza;
quit;

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_sol_allcust
		set country  = 'US'
		where country = 'UNK'
	;
   )
   by netezza;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_sol_allcust
			set petperks = 1
			where customer_eid in (select distinct customer_eid
			from CIDB_USR..CRM_PROD_CUST_EID
			where PETPERKS_LOYAL_FLG = 1
			)
	;
   )
   by netezza;
quit;



proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_sol_allcust
			set active_12m = 1
			where customer_eid in (select distinct customer_eid
			from CIDB_USR..CRM_PROD_CUST_EID
			where ACTIVE12_FLG = 1
			)
	;
   )
   by netezza;
quit;



proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_sol_allcust
			set em = 1
			where customer_eid in (select distinct customer_eid
			from CIDB_USR..CRM_PROD_CUST_EID
			where EM_CONTACTABLE_FLG = 1
			)
	;
   )
   by netezza;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_sol_allcust
			set dm = 1
			where customer_eid in (select distinct customer_eid
			from CIDB_USR..CRM_PROD_CUST_EID
			where DM_CONTACTABLE_FLG = 1
			)
	;
   )
   by netezza;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		create table ca_db_str_segments as

		select 'Email' as segment
		,a.US_tot
		,b.CA_tot
		,c.PR_tot
		,d.US_12m
		,e.CA_12m
		,f.PR_12m

		from 
		(select count(distinct customer_eid) US_tot
		from ca_db_sol_allcust
		where country = 'US'
		and em = 1
		) a,

		(select count(distinct customer_eid) CA_tot
		from ca_db_sol_allcust
		where country = 'CA'
		and em = 1
		) b,

		(select count(distinct customer_eid) PR_tot
		from ca_db_sol_allcust
		where country = 'PR'
		and em = 1
		) c,

		(select count(distinct customer_eid) US_12m
		from ca_db_sol_allcust
		where country = 'US'
		and em = 1
		and active_12m = 1
		) d,

		(select count(distinct customer_eid) CA_12m
		from ca_db_sol_allcust
		where country = 'CA'
		and em = 1
		and active_12m = 1
		) e,

		(select count(distinct customer_eid) PR_12m
		from ca_db_sol_allcust
		where country = 'PR'
		and em = 1
		and active_12m = 1
		) f


		union all


		select 'Direct Mail' as segment
		,a.US_tot
		,b.CA_tot
		,c.PR_tot
		,d.US_12m
		,e.CA_12m
		,f.PR_12m

		from 
		(select count(distinct customer_eid) US_tot
		from ca_db_sol_allcust
		where country = 'US'
		and dm = 1
		) a,

		(select count(distinct customer_eid) CA_tot
		from ca_db_sol_allcust
		where country = 'CA'
		and dm = 1
		) b,

		(select count(distinct customer_eid) PR_tot
		from ca_db_sol_allcust
		where country = 'PR'
		and dm = 1
		) c,

		(select count(distinct customer_eid) US_12m
		from ca_db_sol_allcust
		where country = 'US'
		and dm = 1
		and active_12m = 1
		) d,

		(select count(distinct customer_eid) CA_12m
		from ca_db_sol_allcust
		where country = 'CA'
		and dm = 1
		and active_12m = 1
		) e,

		(select count(distinct customer_eid) PR_12m
		from ca_db_sol_allcust
		where country = 'PR'
		and dm = 1
		and active_12m = 1
		) f

	;
   )
   by netezza;
quit;


data strategic_segments;
	set nz_wrk.ca_db_str_segments;
run;

proc sql;
	create table strategic_segments as
	select *,
		US_TOT - US_12M as US_LAPSED,
		CA_TOT - CA_12M as CA_LAPSED,
		PR_TOT - PR_12M as PR_LAPSED
	from nz_wrk.ca_db_str_segments
	order by segment desc
	;
quit;


proc transpose data = strategic_segments out=strategic_segments_t;
   id segment;
run;

data strategic_segments_2;
	set strategic_segments_t;
	total_contactable = email + direct_mail;
	if substr(_NAME_,4,3) = '12M' then 
	email_percent = email / total_contactable;
run;

proc transpose data = strategic_segments_2 out=strategic_segments_2t (rename=(_NAME_ = Segment));
   id _NAME_;
run;

data strategic_segments_final;
	set strategic_segments_2t (where=(segment ne 'email_percent'));
	if segment = 'total_contactable' then
		segment = 'Total Contactable';
	if segment = 'Direct_Mail' then
		segment = 'Direct Mail';
	format US: CA: PR: comma12.0;
run;


data strategic_segments_em_pc;
	length segment $ 20;
	set strategic_segments_2t (keep=segment US_12M CA_12M PR_12M);
	where segment = 'email_percent';
	if segment = 'email_percent' then 
		segment = 'Email % Contactable';
	format US: CA: PR: percent8.1;
run;



/*
create table em_contactable_hist_CA 
(
fiscal_yr integer,
fiscal_mo_nbr smallint,
fiscal_mo_name varchar(15),
canada_EM bigint,
canada_12m_purch_EM bigint,
canada_DM bigint,
canada_12m_purch_DM bigint
)
;
*/


proc sql noprint;
	select CA_TOT,
			CA_12M

			into :CA_em_tot,
				 :CA_12m_purch_em

	from strategic_segments_final

	where segment = 'Email'
	;
quit;

proc sql noprint;
	select CA_TOT,
			CA_12M

			into :CA_dm_tot,
				 :CA_12m_purch_dm

	from strategic_segments_final

	where segment = 'Direct Mail'
	;
quit;

/* insert new values into history table.  run this only 1x per month.  
	if there are duplicated months, run the delete code:
	delete from em_contactable_hist_CA where fiscal_mo=201510
*/

%let CA_em_tot2 = %qsysfunc(compress(%qsysfunc(translate(%superq(CA_em_tot),%str( ),%str(,)))));
%let CA_12m_purch_em2 = %qsysfunc(compress(%qsysfunc(translate(%superq(CA_12m_purch_em),%str( ),%str(,)))));
%let CA_dm_tot2 = %qsysfunc(compress(%qsysfunc(translate(%superq(CA_dm_tot),%str( ),%str(,)))));
%let CA_12m_purch_dm2 = %qsysfunc(compress(%qsysfunc(translate(%superq(CA_12m_purch_dm),%str( ),%str(,)))));

%put &fiscal_mo &fiscal_mo_nameq &fiscal_mo_name_abbrq &CA_em_tot2 &CA_12m_purch_em2 &CA_dm_tot2 &CA_12m_purch_dm2;



proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&usr user=&userid password=&pwd);
   execute
   (
		insert into em_contactable_hist_CA values
		(&fiscal_mo,&fiscal_mo_nameq,&fiscal_mo_name_abbrq,&CA_em_tot2,&CA_12m_purch_em2,&CA_dm_tot2,&CA_12m_purch_dm2)
		;
   )
   by netezza;
quit;


data em_contactable_hist_CA;
	set nz_usr.em_contactable_hist_CA;
run;

proc sort data=em_contactable_hist_CA;
	by descending fiscal_mo;
quit;

data em_contactable_hist_CA_2;
	set em_contactable_hist_CA;
	rownum+1;
	if rownum le 2;
run;

proc sql;
	select fiscal_mo_name into :last_month
	from em_contactable_hist_CA_2
	where rownum = 2;
quit;

proc transpose data = em_contactable_hist_CA_2 out=em_contactable_hist_CA_2t;
   id fiscal_mo_name;
run;


proc contents data = em_contactable_hist_CA_2t out=xxx noprint position noprint;
run;

proc sort data=xxx;
	by varnum;
quit;


proc sql noprint;
	select name
			into :this_month

	from xxx

	where varnum =2

	;
quit;

proc sql noprint;
	select name
			into :last_month

	from xxx

	where varnum =3

	;
quit;


data em_contactable_hist_CA_3t;
	set em_contactable_hist_CA_2t;
	delta = &this_month - &last_month;
	delta_pc = &this_month / &last_month -1;
	where _NAME_ ne 'rownum' and _NAME_ ne 'FISCAL_MO';
run;

data em_contactable_hist_em;
	set em_contactable_hist_CA_3t;
	if substr(_NAME_,length(_NAME_)-1,2) = 'EM';
	if _NAME_ = 'CANADA_EM' then _NAME_='Canada EM';
	if _NAME_ = 'CANADA_12M_PURCH_EM' then _NAME_='Canada 12 Mo. Purchase EM';
	rename _NAME_ = Email;
	format &fiscal_mo_name &last_month delta comma12.0 delta_pc percent8.1;
run;


data em_contactable_hist_dm;
	set em_contactable_hist_CA_3t;
	if substr(_NAME_,length(_NAME_)-1,2) = 'DM';
	if _NAME_ = 'CANADA_DM' then _NAME_='Canada DM';
	if _NAME_ = 'CANADA_12M_PURCH_DM' then _NAME_='Canada 12 Mo. Purchase DM';
	rename _NAME_ = Direct_Mail;
	format &fiscal_mo_name &last_month delta comma12.0 delta_pc percent8.1;
run;

/*	END Strategic segment counts	*/



/*	Opt outs	*/



proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
	create table ca_db_first_unsub as
		select cast(min(date_trunc('days',b.eventdate))as date) as first_unsub,
		b.subscriberkey
            
    from cidb_usr..et_sendjobs a, cidb_usr..et_unsubs b
		where a.sendid = b.sendid
		and a.canada_flag = 'Y'
		and date_trunc('days',b.eventdate) between '01nov2013' and &dt2q
		and istrue(length(translate(b.subscriberkey,'0123456789',''))=0)

    group by b.subscriberkey

   	;
   )
   by netezza;
quit;



proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table CA_opt_outs as
   select * from connection to netezza
   (
    select b.fiscal_mo,
		count(distinct a.subscriberkey) as opt_outs
            
    from ca_db_first_unsub a, cidb_prd..days b
	where a.first_unsub = b.day_dt

	group by b.fiscal_mo

   );
   disconnect from netezza;
quit;

proc sort data=CA_opt_outs;
	by fiscal_mo;
quit;


/*	END Opt outs	*/


/*	Email contactability history	*/

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table em_contact_chart as
   select * from connection to netezza
   (
		
		select a.* 
		from 

		(select fiscal_mo,
		fiscal_mo_name_abbr,
		canada_12m_purch_em,
		row_number()over(order by fiscal_mo desc) rownum
		from cidb_usr..EM_CONTACTABLE_HIST_CA
		group by fiscal_mo,
		fiscal_mo_name_abbr,
		canada_12m_purch_em
		) a

		where rownum <=10

		order by rownum desc

   );
   disconnect from netezza;
quit;

data em_contact_chart_final;
	merge em_contact_chart (in=x) CA_opt_outs (in=y);
	by fiscal_mo;
	if x=1 and y=1;
run;


/*	END Email contactability history	*/



/*	email engagement	*/


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (

	create table ca_db_email_engage as
		select a.emailname,
		a.campaign_cd,
		b.sendid,
		b.BATCHID,
		a.type,
		a.resend,
		date_trunc('days',b.eventdate) as dt,
		b.SUBSCRIBERKEY,
		c.FISCAL_WK,
		c.FISCAL_MO,
		c.fiscal_mo_name,
		0 as open,
		0 as click,
		0 as bounce,
		0 as unsub

		from cidb_usr..et_sendjobs a, cidb_usr..et_sent b, cidb_prd..DAYS c
		where a.sendid = b.sendid
		and date_trunc('days',b.eventdate) = c.day_dt
		and date_trunc('days',b.eventdate) between &first_dt_yrq and &dt2q
		and (a.type is null or a.type = 'Trigger')
		and a.canada_flag = 'Y'
		and istrue(length(translate(b.subscriberkey,'0123456789',''))=0)
		group by a.emailname,
		a.campaign_cd,
		b.sendid,
		b.BATCHID,
		a.type,
		a.resend,
		dt,
		b.SUBSCRIBERKEY,
		c.FISCAL_WK,
		c.FISCAL_MO,
		c.fiscal_mo_name
		;
   )
   by netezza;
quit;



proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_email_engage a
		set open = 1
		where exists (select b.subscriberkey
						from cidb_usr..et_opens b
						where a.sendid = b.sendid
						and a.batchid = b.batchid
						and a.subscriberkey = b.subscriberkey
						and b.eventdate between a.dt and a.dt+8
					)
   	;
   )
   by netezza;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_email_engage a
		set click = 1
		where exists (select b.subscriberkey
						from cidb_usr..et_clicks b
						where a.sendid = b.sendid
						and a.batchid = b.batchid
						and a.subscriberkey = b.subscriberkey
						and b.eventdate between a.dt and a.dt+8
					)
   	;
   )
   by netezza;
quit;

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_email_engage a
		set bounce = 1
		where exists (select b.subscriberkey
						from cidb_usr..et_bounces b
						where a.sendid = b.sendid
						and a.batchid = b.batchid
						and a.subscriberkey = b.subscriberkey
						and b.eventdate between a.dt and a.dt+8
					)
   	;
   )
   by netezza;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_email_engage a
		set unsub = 1
		where exists (select b.subscriberkey
						from cidb_usr..et_unsubs b
						where a.sendid = b.sendid
						and a.batchid = b.batchid
						and a.subscriberkey = b.subscriberkey
						and b.eventdate between a.dt and a.dt+8
					)
   	;
   )
   by netezza;
quit;

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_email_engage
		set type = 'Campaign' 
		where type is null
   	;
   )
   by netezza;
quit;

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		create table ca_db_em_metrics_allyear as
		select fiscal_mo,
		fiscal_mo_name,
		count(subscriberkey) as sent,
		sum(open) as opens,
		sum(click) as clicks,
		sum(bounce) as bounces,
		sent-bounces as delivers,
		opens/delivers as open_rate,
		clicks/opens as cto_rate

		from ca_db_email_engage

		group by fiscal_mo,
		fiscal_mo_name
		order by fiscal_mo
   	;
   )
   by netezza;
quit;

data ca_db_em_metrics_allyear;
	set nz_wrk.ca_db_em_metrics_allyear;
	keep fiscal_mo fiscal_mo_name open_rate cto_rate;
run;

proc sort data=ca_db_em_metrics_allyear;
	by fiscal_mo;
quit;



proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		create table ca_db_em_metrics_13wks as
		select type,
		fiscal_wk,
		count(subscriberkey) as sent,
		sum(open) as opens,
		sum(click) as clicks,
		sum(bounce) as bounces,
		sent-bounces as delivers,
		opens/delivers as open_rate,
		clicks/opens as cto_rate

		from ca_db_email_engage

		where fiscal_wk between &hist_start and &hist_end

		group by type,
			fiscal_wk
		order by type,
			fiscal_wk
   	;
   )
   by netezza;
quit;


data ca_em_13wks_camp ca_em_13wks_trigg;
	set nz_wrk.ca_db_em_metrics_13wks;
	if type = 'Campaign' then output ca_em_13wks_camp;
	if type = 'Trigger' then output ca_em_13wks_trigg;
	keep type fiscal_wk sent open_rate cto_rate;
run;

proc sort data=ca_em_13wks_camp;
	by fiscal_wk;
quit;

proc sort data=ca_em_13wks_trigg;
	by fiscal_wk;
quit;

/*	end email engagement	*/





/*	Sends per week	*/

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		create table ca_db_sends_per_week as
		select date_trunc('days',a.eventdate) as dt,
			a.sendid,
			a.batchid,
			a.subscriberkey,
			'US' as country

		from cidb_usr..et_sent a

		where date_trunc('days',a.eventdate)between &hist_start_dayq and &dt2q
			and istrue(length(translate(a.subscriberkey,'0123456789',''))=0)

		group by dt,
			a.sendid,
			a.batchid,
			a.subscriberkey

   	;
   )
   by netezza;
quit;

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		update ca_db_sends_per_week
		set country = 'CA'
		where sendid in (select distinct sendid from cidb_usr..et_sendjobs where canada_flag = 'Y')
   	;
   )
   by netezza;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		create table ca_db_sends_per_week_2 as

		select b.fiscal_wk,
			country,
			count(a.subscriberkey) as send_cnt

		from ca_db_sends_per_week a, cidb_prd..days b

		where a.dt = b.day_dt

		group by country,
			b.fiscal_wk
   	;
   )
   by netezza;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		create table ca_db_cust_per_week as

		select b.fiscal_wk,
			country,
			count(distinct a.subscriberkey) as cust_cnt

		from ca_db_sends_per_week a, cidb_prd..days b

		where a.dt = b.day_dt

		group by country,
			b.fiscal_wk
   	;
   )
   by netezza;
quit;



*	merge the datasets and bring them to SAS;
data sends_per_wk;
	merge nz_wrk.ca_db_sends_per_week_2 (in=x) nz_wrk.ca_db_cust_per_week (in=y);
	by fiscal_wk country;
	if x=1 and y=1;
	sends_per_cust = send_cnt/cust_cnt;
run;


/*	END Sends per week	*/


/*	Email clicks	*/

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		
		create table ca_db_email_clicks as
		select a.campaign_cd,
			a.sendid,
			b.subscriberkey,
			b.ALIAS,
			a.dt
		from CA_DB_EMAIL_ENGAGE a, cidb_usr..et_clicks b
		where a.sendid = b.SENDID
			and a.batchid = b.batchid 
			and a.subscriberkey = b.subscriberkey
			and a.type = 'Campaign'
			and a.dt between &dt1q and &dt2q
		group by a.campaign_cd,
			a.sendid,
			b.subscriberkey,
			b.ALIAS,
			a.dt

   	;
   )
   by netezza;
quit;

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		
		create table ca_db_camp_names as
		select a.campaign_cd,
			max(b.campaign_name) as campaign
		from ca_db_email_clicks a, cidb_prd..HH_CAMPAIGN_HIERARCHY b
		where a.campaign_cd = b.campaign_cd
		group by a.campaign_cd

   	;
   )
   by netezza;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		
		create table ca_db_ic_camp_hier as
		select a.campaign_cd,
			max(b.campaign_name) as campaign

		from ca_db_email_clicks a, HH_CAMPAIGN_HIERARCHY_IC b
		where a.campaign_cd = b.campaign_cd
		group by a.campaign_cd


   	;
   )
   by netezza;
quit;

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (
		
		create table ca_db_email_clicks_2 as
		select a.*,
			case
				when a.campaign_cd = b.campaign_cd then b.campaign
				when a.campaign_cd = c.campaign_cd then c.campaign
				else a.campaign_cd
				end as campaign_name
		from ca_db_email_clicks a left join ca_db_camp_names b on a.campaign_cd = b.campaign_cd
									left join ca_db_ic_camp_hier c on a.campaign_cd = c.campaign_cd


   	;
   )
   by netezza;
quit;


proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   create table top_5_campaign_clicks as
   select * from connection to netezza
   (
		
		select a.* 
		from 

		(select campaign_cd
		,dt
		,campaign_name
		,alias
		,count(distinct subscriberkey) as clickers,
		row_number()over(partition by campaign_cd, dt order by clickers desc) rownum
		from ca_db_email_clicks_2
		where alias not like 'http://%'
		and alias not like 'https://%'
		group by campaign_cd
		,campaign_name
		,alias
		,dt) a

		where rownum <=5

		order by dt,
				campaign_cd,
				rownum

   );
   disconnect from netezza;
quit;


proc sql;
	create table top_5_excl as
	select campaign_cd,
			campaign_name,
			dt,
			rownum,
			clickers
		from top_5_campaign_clicks

	where rownum = 1
	and clickers < 10

	;
run;

proc sql;
	create table top_5_campaign_clicks_2 as
	select a.*

	from top_5_campaign_clicks a left join top_5_excl b
			on a.campaign_cd = b.campaign_cd
			and a.campaign_name = b.campaign_name
			and a.dt = b.dt

	where b.campaign_cd is null
		and b.campaign_name is null
		and b.dt is null

	order by dt,
			campaign_cd,
			rownum

	;
run;


data top_5_campaign_clicks_2;
	set top_5_campaign_clicks_2;
	by dt campaign_cd;
	if first.campaign_cd or first.dt then
		campaign_num+1;
run;



proc sql noprint;

   select max(campaign_num) into :max_camp

   from top_5_campaign_clicks_2;

quit;


%macro can_campaigns;

	%do i= 1 %to &max_camp;

		proc sql;

		create table can_camp_&i as

		select campaign_name,
				alias,
				clickers
			from top_5_campaign_clicks_2

			where campaign_num = &i
			;

		quit;
	%end;
%mend can_campaigns;


%can_campaigns;



/*	END Email clicks	*/



/*	Define macro for looping through campaigns
	will be called in ODS output gridded layout	*/



%macro can_campaigns_ods;

	%do i= 1 %to &max_camp;

		proc sql noprint;
			select distinct campaign_name

				into :current_camp

			from can_camp_&i
			;
		quit;

ods region;
	proc report data=can_camp_&i
	style(report) ={rules=none cellspacing=0}
	style(header) ={cellheight=.15in font_size = 5pt}
	style(column) = {font_face='roboto light' cellheight=.15in font_size = 5pt};

	column ALIAS CLICKERS
					;

	define ALIAS			/display "&current_camp" style={cellwidth=230};
	define CLICKERS			/display '#' style={cellwidth=60};

	run;


	%end;
%mend can_campaigns;




/*

graph examples:

https://support.sas.com/documentation/cdl/en/grstatproc/62603/HTML/default/viewer.htm#a003199396.htm
https://support.sas.com/documentation/cdl/en/grstatproc/62603/HTML/default/viewer.htm#sgplot-ov.htm
http://support.sas.com/sassamples/graphgallery/PROC_GCHART_Graph_Types_Charts_Bar.html

*/





* output PDF;

proc template; 
 define style styles.emdashboard; 
 parent = styles.sasweb; 

 style header from header / 
	font_face=ubuntu
	font_size = 10pt 
	just = center 
	vjust = center
	
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
ods pdf notoc file="G:\SASDATA\icooke\updates\CA Email Campaign Dashboard\FM &fiscal_mo CA Email Campaign Dashboard.pdf" color=yes
	style=emdashboard;

ODS escapechar="^"; 

title j=l font= 'Times New Roman' height=25pt color=cx003CA8 "CA Email Campaign Dashboard: &fiscal_mo_name";

ods layout absolute;


ods region width=5in height=0.5in x=0in y=4in;

title j=l font= 'ubuntu' height=15pt color=white bcolor=cx003CA8 'Monthly Email Engagement';
ods text=' ';

ods region width=4in height=0.5in x=6in y=4.5in;

title j=l font= 'ubuntu' height=15pt color=white bcolor=cx003CA8 'Send Frequency';
ods text=' ';



ods region width=10.5in height=2in;

title j=l font= 'ubuntu' height=15pt color=white bcolor=cx003CA8 'Strategic Segments';

proc report data=strategic_segments_final 
	style(report) ={rules=none cellspacing=0}
	style(column) = {font_face='roboto light' cellheight=.25in font_size = 11pt background=cxEDEDED};

	column segment 	 US_TOT CA_TOT PR_TOT
					 US_12M CA_12M PR_12M
					 US_LAPSED CA_LAPSED PR_LAPSED
					;

	define segment			/display "Data thru &fiscal_mo_name" style={cellwidth=350};
	define US_TOT			/display 'US/Total' style={cellwidth=130};
	define CA_TOT			/display 'CA/Total' style={cellwidth=130};
	define PR_TOT			/display 'PR/Total' style={cellwidth=130};
	define US_12M			/display 'US/12M Active' style={cellwidth=130}; 
	define CA_12M			/display 'CA/12M Active' style={cellwidth=130}; 
	define PR_12M			/display 'PR/12M Active' style={cellwidth=130}; 
	define US_LAPSED		/display 'US/Lapsed' style={cellwidth=130}; 
	define CA_LAPSED		/display 'CA/Lapsed' style={cellwidth=130}; 
	define PR_LAPSED		/display 'PR/Lapsed' style={cellwidth=130};


run;



ods region width=6in height=.5in y=1.7in;

proc report data=strategic_segments_em_pc noheader
	style(report) ={cellspacing=0}
	style(column) = {font_face='roboto light' cellheight=.25in font_size = 11pt background=cxEDEDED};

	column segment US_12M CA_12M PR_12M;

	define segment				/display  style={cellwidth=350};
	define US_12M				/display  style={cellwidth=130};
	define CA_12M				/display  style={cellwidth=130};
	define PR_12M				/display  style={cellwidth=130};

run;



ods region width=6in height=1.8in y=2in;

title j=l font= 'ubuntu' height=15pt color=white bcolor=cx003CA8 'EM/DM Change Since Last Month';
proc report data=EM_CONTACTABLE_HIST_EM 
	style(report) ={cellspacing=0}
	style(header) ={cellheight=.25in}
	style(column) = {font_face='roboto light' cellheight=.25in font_size = 11pt background=cxEDEDED};

	column Email &fiscal_mo_name &last_month delta delta_pc;

	define Email					/display 'Email' style={cellwidth=260};
	define &fiscal_mo_name			/display  style={cellwidth=150};
	define &last_month				/display  style={cellwidth=150};
	define delta					/display 'Change' style={cellwidth=150};
	define delta_pc					/display 'Change %' style={cellwidth=150};

run;

proc report data=EM_CONTACTABLE_HIST_DM noheader
	style(report) ={cellspacing=0}
	style(column) = {font_face='roboto light' cellheight=.25in font_size = 11pt background=cxEDEDED};

	column direct_mail &fiscal_mo_name &last_month delta delta_pc;

	define direct_mail					/display 'Direct Mail' style={cellwidth=260};
	define &fiscal_mo_name			/display  style={cellwidth=150};
	define &last_month				/display  style={cellwidth=150};
	define delta					/display  style={cellwidth=150};
	define delta_pc					/display  style={cellwidth=150};

run;





ods region width=4in height=2in x=6in y=5in;
/* 	send frequency chart	*/

ods graphics on /
	width=4in
	height=2in
	border=off
;

proc sgplot data=sends_per_wk NOBORDER;
   title "Avg Sends per Customer";
   yaxis label='Sends per customer';
   xaxis label='Fiscal Week';
   vbar fiscal_wk / response=sends_per_cust group=country groupdisplay=cluster
					barwidth=1 clusterwidth=.5;
	keylegend / noborder location=outside position=bottom;
run;





ods region width=5in height=2.7in x=0in y=4.6in;

ods graphics on /
	width=5in
	height=2.5in
	;
/*	monthly email engagement */

proc sgplot data=ca_db_em_metrics_allyear  NOBORDER ;
  yaxis TICKVALUEFORMAT=percent8.0 display=(NOLABEL);
  xaxis DISCRETEORDER=DATA display=(NOLABEL);
  format open_rate cto_rate percent8.1;
  title "Monthly Open and Click Rates";
  vbar fiscal_mo_name / response=open_rate legendlabel='Open Rate' barwidth=0.7 datalabel
						fillattrs=(color=cx5CACEE)
						DATALABELATTRS= (size=8pt)
						;
  vbar fiscal_mo_name / response=cto_rate legendlabel='CTO Rate' datalabel
                  barwidth=0.3
                  transparency=0.2
					fillattrs=(color=cxFF3030)
					DATALABELATTRS= (color=white size=8pt WEIGHT= BOLD);
  keylegend / noborder location=outside position=bottom;
run;


ods region width=3.5in height=2.5in x=6.5in y=1.7in;

ods graphics on /
	width=3.5in
	height=2in
	border=off
;

title j=l font='ubuntu' height=15pt color=white bcolor=cx003CA8 'Historical Email Counts';
ods text=' ';

/*	email contactability chart	*/

proc sgplot data=em_contact_chart_final NOBORDER;
  yaxis TICKVALUEFORMAT=comma12.0 label='EM Contactable #';
  y2axis TICKVALUEFORMAT=comma12.0 min=0 label='Opt outs';
  xaxis DISCRETEORDER=DATA display=(NOLABEL);
  format canada_12m_purch_em comma12.0;
  title "Email Contactability History";
  vbar fiscal_mo_name_abbr / response=canada_12m_purch_em
					fillattrs=(color=cx5CACEE)
					barwidth=0.5
					legendlabel='EM Contactable';
  vline fiscal_mo_name_abbr / response=opt_outs y2axis 
					legendlabel='Opt outs'
					lineattrs=(color='red' thickness=3);
  keylegend / noborder location=outside position=bottom;
run;






ods layout end;

ods startpage=now;

title;

ods layout absolute;


ods region width=5in height=2.5in ;


ods graphics on /
	width=5in
	height=2.5in
	;

proc sgplot data=CA_EM_13WKS_CAMP;
   title "Campaigns";
   yaxis TICKVALUEFORMAT=comma12.0 label='Emails Sent';
   y2axis TICKVALUEFORMAT=percent8.0 min=0 max=.4 label='Engagement Rates';
   xaxis label='Fiscal Week' type=DISCRETE;
   band x=fiscal_wk upper=sent lower=0 / fillattrs=(color=darkblue) legendlabel='Total Sends';
   series x=fiscal_wk y=open_rate / lineattrs=(thickness=3 color=red) y2axis legendlabel='Open Rate';
   series x=fiscal_wk y=CTO_rate / lineattrs=(thickness=3 color=green) y2axis legendlabel='CTO Rate';
   keylegend / noborder location=outside position=bottom;
run;


ods region width=5in height=2.5in x=5.2in;


proc sgplot data=CA_EM_13WKS_TRIGG;
   title "Triggers/Recurring";
   yaxis TICKVALUEFORMAT=comma12.0 max=100000 label='Emails Sent';
   y2axis TICKVALUEFORMAT=percent8.0 min=0 max=.7 label='Engagement Rates';
   xaxis label='Fiscal Week' type=DISCRETE;
   band x=fiscal_wk upper=sent lower=0 / fillattrs=(color=darkblue) legendlabel='Total Sends';
   series x=fiscal_wk y=open_rate / lineattrs=(thickness=3 color=red) y2axis legendlabel='Open Rate';
   series x=fiscal_wk y=CTO_rate / lineattrs=(thickness=3 color=green) y2axis legendlabel='CTO Rate';
   keylegend / noborder location=outside position=bottom;
run;




ods region width=10in height=.5in y=2.6in;

title j=l font= 'ubuntu' height=15pt color=white bcolor=cx003CA8 "Top 5 Clicks per campaign";
ods text=' ';

ods layout end;



ods layout gridded columns=5 x=0in y=3.1in;

%can_campaigns_ods;




ods layout end;



ods pdf close;




*		Clean up tables.;

proc sql;
   connect to netezza as netezza (server=nzstprdcia01 database=&wrk user=&userid password=&pwd);
   execute
   (

		drop table CA_DB_CAMP_NAMES,CA_DB_CUST_PER_WEEK,CA_DB_EM_METRICS_13WKS,CA_DB_EM_METRICS_ALLYEAR,
					CA_DB_EMAIL_CLICKS,CA_DB_EMAIL_CLICKS_2,CA_DB_EMAIL_ENGAGE,CA_DB_FIRST_UNSUB,CA_DB_SENDS_PER_WEEK,
					CA_DB_SENDS_PER_WEEK_2,CA_DB_SOL_1,CA_DB_SOL_2,CA_DB_SOL_ALLCUST,CA_DB_STR_SEGMENTS,ca_db_ic_camp_hier
		;
   )
   by netezza;
quit;


