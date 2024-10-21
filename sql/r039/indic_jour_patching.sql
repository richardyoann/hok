-- CTE pour lister les utilisateurs et leurs informations organisationnelles
WITH utilisateurs AS (
	SELECT
		DISTINCT
		nni."Matricule__NNI_" ,
		nni.direction,
		nni.division,
		nni.regroupement_unite,
		nni.um,
		nni.dum,
		nni.code_moa
	FROM
		(
			SELECT
				nni."Matricule__NNI_" ,
				COALESCE((regexp_match(nni.direction,'.*\((.*)\)'))[1],'') AS direction,
				COALESCE((regexp_match(nni.division,'.*\((.*)\)'))[1],'') AS division,
				COALESCE((regexp_match(nni.regroupement_unite,'.*\((.*)\)'))[1],'') AS regroupement_unite,
				COALESCE((regexp_match(nni.um,'.*\((.*)\)'))[1],'') AS um,
				COALESCE((regexp_match(nni.dum,'.*\((.*)\)'))[1],'') AS dum,
				COALESCE((regexp_match(nni.code_moa,'.*\((.*)\)'))[1],'') AS code_moa
			FROM use_case_specif.ref_utilisateurs nni
		UNION ALL
			SELECT				
				nni."Matricule__NNI_" ,
				COALESCE((regexp_match(nni.direction,'.*\((.*)\)'))[1],'') AS direction,
				COALESCE((regexp_match(nni.division,'.*\((.*)\)'))[1],'') AS division,
				COALESCE((regexp_match(nni.regroupement_unite,'.*\((.*)\)'))[1],'') AS regroupement_unite,
				COALESCE((regexp_match(nni.um,'.*\((.*)\)'))[1],'') AS um,
				COALESCE((regexp_match(nni.dum,'.*\((.*)\)'))[1],'') AS dum,
				COALESCE((regexp_match(nni.code_moa,'.*\((.*)\)'))[1],'') AS code_moa
			FROM use_case_specif.ref_departements nni
		) nni
	WHERE
		"Matricule__NNI_" IS NOT NULL
		AND trim("Matricule__NNI_") <> ''
)
-- Requête principale pour obtenir les statistiques de déploiement
SELECT
	'Patching' AS indicateur,
	'Patching' AS indicateur_parent,
	concat(	uti.direction,'/' ,uti.division ,'/' ,uti.regroupement_unite ,'/' ,uti.um ,	'/' ,uti.dum ,'/' ,uti.code_moa	) maille,
	'groupes mailles direction/division/regroupement_unite/um/dum/code rc' AS maille_parent,
	count( DISTINCT win.alm_asset_asset_tag_display_value) AS valeur
FROM use_case_specif.vue_imi_dcw_postes_win win
LEFT JOIN utilisateurs uti 
	ON	upper(uti."Matricule__NNI_") = upper(win.nni)
GROUP BY concat(uti.direction,'/' ,uti.division ,'/' ,uti.regroupement_unite ,'/' ,uti.um ,	'/' ,uti.dum ,'/' ,uti.code_moa	)
