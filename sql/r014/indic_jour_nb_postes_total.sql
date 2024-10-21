-- CTE pour lister les utilisateurs et leurs informations organisationnelles
WITH utilisateurs AS (
	SELECT
		DISTINCT
		nni."Matricule__NNI_" ,
		nni.direction,
		nni.division,
		nni.um,
		nni.dum,
		nni.code_moa
	FROM
		(
			SELECT
				nni."Matricule__NNI_" ,
				COALESCE((regexp_match(nni.direction,'.*\((.*)\)'))[1],'') direction,
				COALESCE((regexp_match(nni.division,'.*\((.*)\)'))[1],'') division,
				COALESCE((regexp_match(nni.um,'.*\((.*)\)'))[1],'') um,
				COALESCE((regexp_match(nni.dum,'.*\((.*)\)'))[1],'') dum,
				COALESCE((regexp_match(nni.code_moa,'.*\((.*)\)'))[1],'') code_moa
			FROM
				use_case_specif.ref_utilisateurs nni
		UNION ALL
			SELECT
				nni."Matricule__NNI_" ,
				COALESCE((regexp_match(nni.direction,'.*\((.*)\)'))[1],'') direction,
				COALESCE((regexp_match(nni.division,'.*\((.*)\)'))[1],'') division,
				COALESCE((regexp_match(nni.um,'.*\((.*)\)'))[1],'') um,
				COALESCE((regexp_match(nni.dum,'.*\((.*)\)'))[1],'') dum,
				COALESCE((regexp_match(nni.code_moa,'.*\((.*)\)'))[1],'') code_moa
			FROM
				use_case_specif.ref_departements nni
		) nni
	WHERE "Matricule__NNI_" IS NOT NULL
	AND trim("Matricule__NNI_") <> ''
)

-- Requête principale pour obtenir les statistiques de déploiement
SELECT
	'Nb postes total' AS indicateur,
	'Nb postes total' AS indicateur_parent,
	CONCAT(	uti.direction,'/' ,uti.division  ,'/' ,uti.um ,'/' ,uti.dum ,'/' ,uti.code_moa) maille,
	'groupes mailles direction/division/um/dum/code rc' AS maille_parent,
	COUNT(DISTINCT cp.alm_asset_asset_tag_display_value) AS valeur
FROM  use_case_specif.vue_imi_postes_win_infogere_et_metiers cp 
JOIN utilisateurs uti 
ON	upper(uti."Matricule__NNI_") = upper(cp.user_user_name_display_value)
-- Filtre pour ne prendre que les postes vus dans le parc durant les 30 derniers jours 
WHERE (current_date ::date - cp.lastscan::date ) ::integer < 31		
GROUP BY CONCAT(	uti.direction,'/' ,uti.division  ,'/' ,uti.um ,'/' ,uti.dum ,'/' ,uti.code_moa);
