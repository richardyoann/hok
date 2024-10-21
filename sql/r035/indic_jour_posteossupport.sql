with
  user_coderc as
(
	select
		--  coalesce (nullif(trim(cc.um), '') , nullif(trim(bb.um) , ''), trim(aa.um )) as um
		--  coalesce (nullif(trim(cc.um), '') , nullif(trim(aa.um) , '')) as um
--		  nullif(trim(cc.um), '') as um
--
--		, coalesce (nullif(trim(cc.dum), ''), nullif(trim(bb.dum), ''), trim(aa.dum)) as dum
--		, coalesce (nullif(trim(cc.code_moa), ''),   trim(bb.code_moa)) as code_moa
--		, coalesce (upper(user_user_name_display_value), upper(resp_nni)) as nni
		
		  trim(cc.um) as um
		, trim(cc.dum) as dum
		, trim(cc.code_moa) as code_moa
		, trim(upper(user_user_name_display_value)) as user_user_name_display_value
		
	, alm_asset_asset_tag_display_value
--	, cmdb_ci_actif_os_display_value
	, alm_asset_sys_updated_on_display_value
	, "lastLogonTimestamp"
	, "operatingSystem" as osfullname
	--, dd.cleos 
	, aa."operatingSystem" 
	, ("operatingSystem"::text || ' '::text || "operatingSystemVersion"::text) AS cleos_atlas
	 
	, CASE
		WHEN ee."isProtected" = 0 THEN 'non'
		WHEN ee."isProtected" = 1 THEN '6 mois'
		WHEN ee."isProtected" = 2 THEN 'oui'
	  END AS "support"
	
	
	--,*
	from use_case_specif.vue_imi_dcw_postes_vm_win aa
	left join  use_case_specif.vue_donnees_utilisateurs_departements bb
	on aa.resp_nni = bb."Matricule__NNI_"
	left join  use_case_specif.vue_donnees_utilisateurs_departements cc
	on aa.user_user_name_display_value = cc."Matricule__NNI_"
	LEFT join
	( select * 
		, CASE
			WHEN "ref_s12_Version_OS_Windows".groupid::text = 'AF126CAE-171C-4329-B1AB-D70F55CA7EEA'::text THEN "ref_s12_Version_OS_Windows".os_nom_commercial::text || ' ESU Year 1'::text
			WHEN "ref_s12_Version_OS_Windows".groupid::text = '5BE7EFCD-462C-4B18-9FEA-B6F7A4B7B71A'::text THEN "ref_s12_Version_OS_Windows".os_nom_commercial::text || ' ESU Year 2'::text
			WHEN "ref_s12_Version_OS_Windows".groupid::text = 'C62D695A-19F2-42DB-B732-5BFB503BDEC0'::text THEN "ref_s12_Version_OS_Windows".os_nom_commercial::text || ' ESU Year 3'::text
			WHEN "ref_s12_Version_OS_Windows".groupid::text = '66678789-058A-4DDF-B658-1698405CA5B2'::text THEN "ref_s12_Version_OS_Windows".os_nom_commercial::text || ' ESU Year 1'::text
			WHEN "ref_s12_Version_OS_Windows".groupid::text = '0110AA20-8307-4A4A-A91C-A8EB790714FF'::text THEN "ref_s12_Version_OS_Windows".os_nom_commercial::text || ' ESU Year 2'::text
			WHEN "ref_s12_Version_OS_Windows".groupid::text = 'A016C6E2-83F4-4086-9CD4-82FAB8418F7E'::text THEN "ref_s12_Version_OS_Windows".os_nom_commercial::text || ' ESU Year 3'::text
			ELSE (regexp_replace("ref_s12_Version_OS_Windows".os_nom_commercial::text, 'Microsoft.? '::text, ''::text) || ' '::text) || "ref_s12_Version_OS_Windows".os_versiontechnique::text
		END AS cleos
		from use_case_specif."ref_s12_Version_OS_Windows"
	) dd
	ON dd.cleos  = ("operatingSystem"::text || ' '::text || "operatingSystemVersion"::text) --cleos_atlas 
	LEFT JOIN use_case_specif.vue_s12_LifecycleProductGroups_protection ee
	ON dd.groupid = ee."GroupID"
)

--select * from user_coderc

--select count(alm_asset_asset_tag_display_value) from user_coderc  --92535 identique SCOPE/ITAM
--where cmdb_ci_actif_os_display_value ~ 'Windows 10' --91487 / 89614 dans le rapport
--where cmdb_ci_actif_os_display_value ~ 'Windows 7' --1125 / 416 dans le rapport


--select count(distinct  alm_asset_asset_tag_display_value) from user_coderc 
--where nullif(trim(alm_asset_asset_tag_display_value),'')  is not null --93252 / 93247 dans le rapport  identique SCOPE/ITAM


--select count(distinct alm_asset_asset_tag_display_value)
--from user_coderc
--where coalesce((regexp_match(um, '\((.*)\)'))[1],'') = '6520M'

--select * from use_case_specif.vue_imi_dcw_postes_vm_win aa

--select distinct(alm_asset_sys_updated_on_display_value)
--from user_coderc


select indicateur, indicateur_parent, maille, 'groupes mailles um/dum/code rc' as maille_parent, sum(valeur) as valeur
from(
	select
	  trim('nombre de postes avec support ' || coalesce (nullif ("support", ''), 'inconnu') ) as indicateur
	, 'nombre de postes avec support' as indicateur_parent
	, (coalesce((regexp_match(zz.um, '.*\((.*)\)'))[1],'')
	|| '/' || coalesce((regexp_match(zz.dum, '\((.*)\)'))[1],'')
	|| '/' || coalesce((regexp_match(zz.code_moa , '\((.*)\)'))[1],'')) as maille
	, 1 as valeur
	from(
		  select trim(osfulllist[1]) as "osname", trim(osfulllist[2]) as "version"--, trim(osfulllist[3]) as "license", (trim(osfulllist[4])~'LTS.') as "lts"
	, os2.um, os2.dum, os2.code_moa 
	, alm_asset_asset_tag_display_value
	, support 
	--    , *
	from
	(
		select (regexp_match(os.osfull, '(\S+)\s+(\S+)\s+(\S+)\s*(\S*)$')) as os_lic
	, os.um, os.dum, os.code_moa
	, os.alm_asset_asset_tag_display_value 
	, os.support 
	
	from(
		select
		  distinct(alm_asset_asset_tag_display_value)
		, REGEXP_REPLACE(
			REGEXP_REPLACE(
				REGEXP_REPLACE(
				  osfullname
				, '(Entreprise)', 'Enterprise', 'g')
		, '[^\w\s\.]', '', 'g')
		, '(Microsoft|Ã¢â€žÂ¢|Ã‚Â®|2016)', '', 'g') as osfull
		, osfullname
		, um, dum, code_moa, support 
		from user_coderc xxxxx
	) os
	) as os2(osfulllist)
	) zz
) zzz
group by indicateur_parent, indicateur, maille


--SELECT id, os_nom_commercial, os_versiontechnique, os_versioncommerciale, osedition, osname, groupid, date_publication, statut_maj, date_maj
--FROM use_case_specif."ref_s12_Version_OS_Windows";


--select nni , resp_nom_prenom , nom_prenom , user_user_name_display_value , user_resp_user_name_display_value
--from
--use_case_specif.vue_imi_dcw_postes_vm_win aa
