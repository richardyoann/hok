with
  user_coderc as
(
	select
		  trim(cc.um) as um
		, trim(cc.dum) as dum
		, trim(cc.code_moa) as code_moa
		, trim(upper(user_user_name_display_value)) as user_user_name_display_value
		, alm_asset_asset_tag_display_value
--		, alm_asset_sys_updated_on_display_value
--		, "lastLogonTimestamp"
		, rf.osname  as osfullname
		, rf.osedition 
	from use_case_specif.vue_imi_dcw_postes_vm_win aa
	left join  use_case_specif.vue_donnees_utilisateurs_departements cc
	on aa.user_user_name_display_value = cc."Matricule__NNI_"
	left join use_case_specif.vue_ref_s12_version_os_windows_cleos rf
	on concat("operatingSystem",' ',"operatingSystemVersion")= rf.cleos
)

select indicateur, indicateur_parent, maille, 'groupes mailles um/dum/code rc' as maille_parent, sum(valeur) as valeur
from(
	select
	  distinct(alm_asset_asset_tag_display_value), osfullname, osedition
	, trim('nombre de postes avec licence ' || coalesce (nullif (osedition, ''), 'inconnu')) as indicateur
	, 'nombre de postes avec licence' as indicateur_parent
	, (coalesce((regexp_match(um, '.*\((.*)\)'))[1],'')
	|| '/' || coalesce((regexp_match(dum, '\((.*)\)'))[1],'')
	|| '/' || coalesce((regexp_match(code_moa , '\((.*)\)'))[1],'')) as maille
	, 1 as valeur
	, um, dum, code_moa
	from user_coderc xx
) zzz
group by indicateur_parent, indicateur, maille



