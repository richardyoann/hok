with utilisateurs as (
select
	distinct
nni."Matricule__NNI_" ,
	nni.direction,
	nni.division,
	nni.regroupement_unite,
	nni.um,
	nni.dum,
	nni.code_moa
from
	(
	select
		nni."Matricule__NNI_" ,
		coalesce((regexp_match(nni.direction, '.*\((.*)\)'))[1], '') direction,
		coalesce((regexp_match(nni.division, '.*\((.*)\)'))[1], '') division,
		coalesce((regexp_match(nni.regroupement_unite, '.*\((.*)\)'))[1], '') regroupement_unite,
		coalesce((regexp_match(nni.um, '.*\((.*)\)'))[1], '') um,
		coalesce((regexp_match(nni.dum, '.*\((.*)\)'))[1], '') dum,
		coalesce((regexp_match(nni.code_moa, '.*\((.*)\)'))[1], '') code_moa
	from
		use_case_specif.ref_utilisateurs nni
union all
	select
		nni."Matricule__NNI_" ,
		coalesce((regexp_match(nni.direction, '.*\((.*)\)'))[1], '') direction,
		coalesce((regexp_match(nni.division, '.*\((.*)\)'))[1], '') division,
		coalesce((regexp_match(nni.regroupement_unite, '.*\((.*)\)'))[1], '') regroupement_unite,
		coalesce((regexp_match(nni.um, '.*\((.*)\)'))[1], '') um,
		coalesce((regexp_match(nni.dum, '.*\((.*)\)'))[1], '') dum,
		coalesce((regexp_match(nni.code_moa, '.*\((.*)\)'))[1], '') code_moa
	from
		use_case_specif.ref_departements nni 
) nni
where
	"Matricule__NNI_" is not null
	and trim("Matricule__NNI_") <> ''
),
mecm as (
select
	distinct on
	("Workstation") "Workstation" ,
	upper(split_part("User_Top_NNI", '-', 1)) as user_top_nni ,
	top_user.um
from
	use_case_specif.vue_s12_workstations_infos a
left join utilisateurs top_user on
	upper(split_part(a."User_Top_NNI", '-', 1)) = top_user."Matricule__NNI_"
order by
	"Workstation" ,
	"Last_Scan" desc 
),
itam as (
select 
	trim(upper(win.alm_asset_asset_tag_display_value)) as alm_asset_asset_tag_display_value,
	uti.um as uti_um,
	uti.dum as uti_dum,
	uti.direction,
	uti.division,
	uti.regroupement_unite,
	uti.code_moa,
	case
		when s.lastscan is null then 'jamais vu'
		else 
	case
			when (current_date ::date - s.lastscan ::date ) ::integer < 8 then 'vu'
			else 'pas vu'
		end
	end as vu
from
	use_case_specif.vue_imi_dcw_postes_win win
left join utilisateurs uti on
	upper(uti."Matricule__NNI_") = upper(win.nni)
left join use_case_specif.vue_lastscan s on
	s.hostname = win.alm_asset_asset_tag_display_value 
 ),
justesse as (
select
	i.alm_asset_asset_tag_display_value,
	i.direction,
	i.division,
	i.regroupement_unite,
	uti_um,
	uti_dum,
	i.code_moa,
	case
		when mecm.um is null
			or uti_um is null then 3
			when mecm.um = uti_um then 1
			else 2
		end as j_entite_utilisateur,
		vu
	from
		itam i
	left join mecm on
		mecm."Workstation" = i.alm_asset_asset_tag_display_value
)
select
	'Nb postes entite utilisateur frais' as indicateur,
	'Nb postes' as indicateur_parent,
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , uti_um , '/' , uti_dum , '/' , code_moa) maille,
	'groupes mailles direction/division/regroupement_unite/um/dum/code rc' as maille_parent,
	sum(case when j_entite_utilisateur = 1 and vu = 'vu' then 1 else 0 end ) as valeur
from
	justesse f
group by
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , uti_um , '/' , uti_dum , '/' , code_moa)
