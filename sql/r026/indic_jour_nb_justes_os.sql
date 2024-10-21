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
ad as (
select 
	distinct on ("sAMAccountName") "sAMAccountName" as "computer",
	"lastLogonTimestamp" as "lastLogonTimestamp",
	"operatingSystem" as "operatingSystem"
from
	use_case_specif.vue_dcw_computer
order by
	"sAMAccountName" ,
	"lastLogonTimestamp" desc
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
	win.cmdb_ci_actif_os_display_value
from
	use_case_specif.vue_imi_dcw_postes_win win
left join utilisateurs uti on
	upper(uti."Matricule__NNI_") = upper(win.nni)
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
		when ad."operatingSystem" is null
			or cmdb_ci_actif_os_display_value is null
			or trim(ad."operatingSystem" ) = ''
				or trim(cmdb_ci_actif_os_display_value) = '' 
then 3
				when trim(upper(ad."operatingSystem")) = trim(upper(cmdb_ci_actif_os_display_value))
					or concat('MICROSOFT ', replace(trim(upper(ad."operatingSystem")), 'ENTREPRISE', 'ENTERPRISE')) = trim(upper(cmdb_ci_actif_os_display_value))
						or concat('MICROSOFT ', replace(trim(upper(ad."operatingSystem")), 'ENTERPRISE', 'ENTREPRISE')) = trim(upper(cmdb_ci_actif_os_display_value))
then 1
						else 2
					end as j_os
				from
					itam i
				left join ad on
					ad."computer" = i.alm_asset_asset_tag_display_value
) 
select
	'Nb postes os juste' as indicateur,
	'Nb postes' as indicateur_parent,
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , uti_um , '/' , uti_dum , '/' , code_moa) maille,
	'groupes mailles direction/division/regroupement_unite/um/dum/code rc' as maille_parent,
	sum(case when j_os = 1 then 1 else 0 end ) as valeur
from
	justesse f
group by
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , uti_um , '/' , uti_dum , '/' , code_moa)