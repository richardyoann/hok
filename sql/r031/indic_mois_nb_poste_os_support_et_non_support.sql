with kpi as (
select
	id_calc,
	id_maille,
	"date",
	extract(month
from
	"date" ) num_mois ,
	valeur
from
	use_case_specif.hcal_dfa_hok_jours
),indicateur as (
select
	thc.id_calc ,
	thc."label" ,
	thc.rapports
from
	use_case_specif.hcal_dfa_hok_calc thc 
),maille as (
select
	thm.id_maille ,
	thm."label"
from
	use_case_specif.hcal_dfa_hok_mailles thm 
)
select
	distinct p.id_calc,
	p.id_maille,
	i."label" label_indicateur,
	m."label" maille,
	avg(p.valeur) valeur,
	date_trunc('month', (current_date - interval '1 month'))::date as "date"
from
	kpi p
left join indicateur i on
	p.id_calc = i.id_calc
left join maille m on
	p.id_maille = m.id_maille
where
	num_mois = extract(month from current_date - interval '1 month')	and 'R031' = any(i.rapports)-- filtrage des kpi de ce cas la
group by i."label" , i.rapports , m."label" , p.num_mois, p.id_calc,p.id_maille


