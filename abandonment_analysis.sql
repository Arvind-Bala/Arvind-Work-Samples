WITH
overall_student_usage AS (
SELECT
  cs.coach_session_id,
  cs.coach_session_scheduled,
  student.user_id as student_user_id,
  FLOOR(DATEDIFF(hours,cs.coach_session_created,cs.coach_session_scheduled)*1.00 / 24) as days_to_session,
  CASE
    WHEN cs.coach_session_abandoned IS NOT NULL
    AND cs.accepted_user_id IS NOT NULL
    AND cs.cancelled_user_id = cs.student_user_id
      THEN 1
    ELSE 0
  END AS is_student_abandoned,
  CASE
    WHEN cs.coach_session_abandoned IS NOT NULL
    AND cs.cancelled_user_id = cs.coach_user_id
      THEN 1
    ELSE 0
  END AS is_coach_abandoned,
  CASE
    WHEN cs.coach_session_ended IS NOT NULL
      THEN 1
    ELSE 0
  END AS is_completed_session,
  COALESCE(
    SUM(
      CASE
        WHEN cs.coach_session_abandoned IS NOT NULL
        AND cs.accepted_user_id IS NOT NULL
        AND cs.cancelled_user_id = cs.student_user_id
          THEN 1
        ELSE 0
      END
    ) OVER (
      PARTITION BY student.user_id
      ORDER BY cs.coach_session_created
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ),0
  ) as prior_sessions_abandoned,
  COALESCE(
    SUM(
      CASE
        WHEN cs.coach_session_ended IS NOT NULL
          THEN 1
        ELSE 0
      END
    ) OVER (
      PARTITION BY student.user_id
      ORDER BY cs.coach_session_created
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ),0
  ) as prior_sessions_completed,
  ROW_NUMBER() OVER (
    PARTITION BY student.user_id
    ORDER BY cs.coach_session_created) as session_rnk,
  ROW_NUMBER() OVER (
    PARTITION BY cs.student_user_id, (
      CASE
        WHEN cs.coach_session_abandoned IS NOT NULL
        AND cs.accepted_user_id IS NOT NULL
        AND cs.cancelled_user_id = cs.student_user_id
          THEN 1
        ELSE 0
      END
      )
    ORDER BY cs.coach_session_created) as abandon_completed_rnk
FROM
  coach_sessions cs
  INNER JOIN users student
    ON student.user_id = cs.student_user_id
  INNER JOIN users coach
    ON coach.user_id = cs.coach_user_id
    AND coach.school_id = [get_upswing_school_id]
  INNER JOIN schools s
    ON s.school_id = student.school_id
)

SELECT
  CASE
    WHEN days_to_session <= 5
      THEN CAST(days_to_session AS TEXT)
    ELSE '6+'
  END as days_to_session,
  SUM(is_student_abandoned) as student_abandoned,
  SUM(is_completed_session) as completed,
  COUNT(DISTINCT coach_session_id) as total_scheduled,
  SUM(is_student_abandoned)*1.00 / COUNT(DISTINCT coach_session_id) as abandon_rate
FROM
  overall_student_usage
WHERE
  session_rnk = 1
GROUP BY 1
ORDER BY 1
