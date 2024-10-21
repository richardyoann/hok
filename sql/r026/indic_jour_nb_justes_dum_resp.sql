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
itam as (
select 
	trim(upper(win.alm_asset_asset_tag_display_value)) as alm_asset_asset_tag_display_value,
	uti.um as uti_um,
	uti.dum as uti_dum,
	uti.direction,
	uti.division,
	uti.regroupement_unite,
	uti.code_moa,
	split_part(split_part(resp.dum, '(', 2), ')', 1) as resp_dum,
	case
		when LENGTH(win.nni)= 6
			or LENGTH(win.nni)= 8 then 'PAS DE SERVICE'
			else nni
		end NNI_DE_SERVICE
	from
		use_case_specif.vue_imi_dcw_postes_win win
	left join utilisateurs uti on
		upper(uti."Matricule__NNI_") = upper(win.nni)
	left join utilisateurs resp on
		upper(resp."Matricule__NNI_") = upper(win.user_resp_user_name_display_value)
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
		when nni_de_service = 'PAS DE SERVICE' then 4
		else case
			when uti_dum is null
				or trim(uti_dum) = ''
					or resp_dum is null
					or trim(resp_dum) = ''
then 3
					when uti_dum = resp_dum then 1
					else 2
				end
			end as j_dum_resp
		from
			itam i 
) 
select
	'Nb postes dum resp juste' as indicateur,
	'Nb postes' as indicateur_parent,
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , uti_um , '/' , uti_dum , '/' , code_moa) maille,
	'groupes mailles direction/division/regroupement_unite/um/dum/code rc' as maille_parent,
	sum(case when j_dum_resp = 1 then 1 else 0 end ) as valeur
from
	justesse f
group by
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , uti_um , '/' , uti_dum , '/' , code_moa)