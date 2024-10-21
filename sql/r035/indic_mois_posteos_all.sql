with 
  kpi as ( 
	SELECT id_calc, id_maille, "date", EXTRACT(MONTH FROM "date" ) num_mois , valeur
	FROM use_case_specif.hcal_dfa_hok_jours
)
, indicateur as ( 
	select thc.id_calc , thc."label" , thc.rapports--, id_parent 
	from use_case_specif.hcal_dfa_hok_calc thc 
)
, maille as ( 
	select thm.id_maille ,thm."label" 
	from use_case_specif.hcal_dfa_hok_mailles thm 
)


select  p.id_calc,p.id_maille,i."label" label_indicateur
--, i.rapports
, m."label" maille
--, p.num_mois
,avg(p.valeur) valeur
, date_trunc('month', (current_date - interval '1 month'))::date as "date"


--,km.num_mois, avg (km.valeur) moyen_mois_m_1 , km.id_calc,km.id_maille
--, i.id_parent
from kpi p
left join indicateur i on p.id_calc = i.id_calc
left join maille m on p.id_maille = m.id_maille
--left join kpi km on ((p.num_mois-1) = km.num_mois and km.id_calc = p.id_calc and  km.id_maille = p.id_maille )
where num_mois = extract(month from current_date - interval '1 month')
and i."label" ~'nombre de postes avec (OS|support|licence|LTSC)' -- filtrage des kpi de ce cas la
group by i."label" , i.rapports , m."label" , p.num_mois, p.id_calc,p.id_maille--, i.id_parent--, km.num_mois, km.id_calc,km.id_maille

