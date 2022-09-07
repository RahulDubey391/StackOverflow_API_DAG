FACT_ANSWERS_QUERY = '''INSERT INTO `business-deck.STACK_OVERFLOW_SCHEMA.FCT_ANSWERS`
SELECT  
CASE
  WHEN is_accepted = 'nan' THEN NULL
  ELSE CAST(is_accepted AS BOOL) 
END AS is_accepted,
CASE 
  WHEN score = 'nan' THEN NULL
  ELSE CAST(score AS INTEGER) 
END AS score,
CASE
  WHEN last_activity_date = 'nan' THEN NULL
  ELSE CAST(last_activity_date AS DATETIME) 
END AS last_activity_date,
CASE 
  WHEN last_edit_date = 'nan' THEN NULL
  ELSE CAST(last_edit_date AS DATETIME) 
END AS last_edit_date,
CASE 
  WHEN creation_date = 'nan' THEN NULL
  ELSE CAST(creation_date AS DATETIME) 
END AS creation_date,
CASE
  WHEN answer_id = 'nan' THEN NULL
  ELSE CAST(answer_id AS INTEGER) 
END AS answer_id,
CASE 
  WHEN question_id = 'nan' THEN NULL 
  ELSE CAST(question_id AS INTEGER) 
END AS question_id,
CASE 
  WHEN content_license = 'nan' THEN NULL
  ELSE CAST(content_license AS STRING) 
END AS content_license,
CASE 
  WHEN owner_reputation = 'nan' THEN NULL
  ELSE CAST(owner_reputation AS INTEGER) 
END AS owner_reputation,
CASE 
  WHEN owner_user_id = 'nan' THEN NULL
  ELSE CAST(owner_user_id AS INTEGER) 
END AS owner_user_id,
CASE 
  WHEN owner_user_type = 'nan' THEN NULL
  ELSE CAST(owner_user_type AS STRING) 
END AS owner_user_type,
CASE
  WHEN owner_accept_rate = 'nan' THEN NULL
  ELSE CAST(owner_accept_rate AS DECIMAL) 
END AS owner_accpet_rate,
CASE 
  WHEN owner_profile_image = 'nan' THEN NULL
  ELSE CAST(owner_profile_image AS STRING) 
END AS owner_profile_image,
CASE 
  WHEN owner_display_name = 'nan' THEN NULL
  ELSE CAST(owner_display_name AS STRING) 
END AS owner_diplay_name,
CASE
  WHEN owner_link = 'nan' THEN NULL
  ELSE CAST(owner_link AS STRING) 
END AS owner_link,
CASE 
  WHEN community_owned_date = 'nan' THEN NULL
  ELSE CAST(community_owned_date AS DATETIME) 
END AS community_owned_date
FROM `business-deck.STACK_OVERFLOW_SCHEMA.STG_ANSWERS`;'''

FACT_QUESTIONS_QUERY = '''INSERT INTO `business-deck.STACK_OVERFLOW_SCHEMA.FCT_QUESTIONS`
SELECT  
CASE WHEN tags='nan' THEN NULL ELSE CAST(tags AS STRING) END AS tags,
CASE WHEN is_answered='nan' THEN NULL ELSE CAST(is_answered AS BOOL) END AS is_answered,
CASE WHEN view_count='nan' THEN NULL ELSE CAST(view_count AS INTEGER) END AS view_count,
CASE WHEN answer_count='nan' THEN NULL ELSE CAST(answer_count AS INTEGER) END AS answer_count,
CASE WHEN score='nan' THEN NULL ELSE CAST(score AS DECIMAL) END AS score,
CASE WHEN last_activity_date='nan' THEN NULL ELSE CAST(last_activity_date AS DATETIME) END AS last_activity_date,
CASE WHEN creation_date='nan' THEN NULL ELSE CAST(creation_date AS DATETIME) END AS creation_date,
CASE WHEN last_edit_date='nan' THEN NULL ELSE CAST(last_edit_date AS DATETIME) END AS last_edit_date,
CASE WHEN question_id='nan' THEN NULL ELSE CAST(question_id AS INTEGER) END AS question_id,
CASE WHEN content_license='nan' THEN NULL ELSE CAST(content_license AS STRING) END AS content_license,
CASE WHEN link='nan' THEN NULL ELSE CAST(link AS STRING) END AS link,
CASE WHEN title='nan' THEN NULL ELSE CAST(title AS STRING) END AS title,
CASE WHEN owner_reputation='nan' THEN NULL ELSE CAST(owner_reputation AS INTEGER) END AS owner_reputation,
CASE WHEN owner_user_id='nan' THEN NULL ELSE CAST(owner_user_id AS INTEGER) END AS owner_user_id,
CASE WHEN owner_user_type='nan' THEN NULL ELSE CAST(owner_user_type AS STRING) END AS owner_user_type,
CASE WHEN owner_profile_image='nan' THEN NULL ELSE CAST(owner_profile_image AS STRING) END AS owner_profile_image,
CASE WHEN owner_display_name='nan' THEN NULL ELSE CAST(owner_display_name AS STRING) END AS owner_display_name,
CASE WHEN owner_link='nan' THEN NULL ELSE CAST(owner_link AS STRING) END AS owner_link,
CASE WHEN accepted_answer_id='nan' THEN NULL ELSE CAST(accepted_answer_id AS DECIMAL) END AS accepted_answer_id,
CASE WHEN closed_date='nan' THEN NULL ELSE CAST(closed_date AS DATETIME) END AS closed_date,
CASE WHEN closed_reason='nan' THEN NULL ELSE CAST(closed_reason AS STRING) END AS closed_reason,
CASE WHEN owner_accept_rate='nan' THEN NULL ELSE CAST(owner_accept_rate AS DECIMAL) END AS owner_accept_rate
FROM `business-deck.STACK_OVERFLOW_SCHEMA.STG_QUESTIONS`;'''


SNAP_FACT_ANSWERS = '''INSERT INTO `business-deck.STACK_OVERFLOW_SCHEMA.SNAP_FCT_ANSWERS`
SELECT
CURRENT_DATETIME() AS LOAD_TIMESTAMP,
T1.*
FROM `business-deck.STACK_OVERFLOW_SCHEMA.FCT_ANSWERS` AS T1;'''

SNAP_FACT_QUESTIONS = '''INSERT INTO `business-deck.STACK_OVERFLOW_SCHEMA.SNAP_FCT_QUESTIONS`
SELECT
CURRENT_DATETIME() AS LOAD_TIMESTAMP,
T1.*
FROM `business-deck.STACK_OVERFLOW_SCHEMA.FCT_QUESTIONS` AS T1'''