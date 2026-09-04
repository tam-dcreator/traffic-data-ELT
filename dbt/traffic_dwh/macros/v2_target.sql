{#
  V2 target detection + environment derivation.
  ─────────────────────────────────────────────
  The V2 dbt target naming convention is:  v2_<neon-branch-name>

    v2_dev, v2_staging, v2_production, v2_feature_x, ...

  The suffix after `v2_` is the Neon branch/environment being targeted.

  V2 detection is based on the explicit `v2_` PREFIX — not a hardcoded list of
  branch names and not a loose `'v2' in target.name` substring test (which could
  misclassify unrelated targets such as `myv2_local`).
#}

{% macro is_v2_target() %}
  {#- True when the current target name begins with the literal prefix 'v2_'. -#}
  {{ return(target.name.startswith('v2_')) }}
{% endmacro %}


{% macro v2_environment() %}
  {#-
    Return the logical environment/branch suffix for a V2 target, e.g.

        v2_dev        → dev
        v2_staging    → staging
        v2_production → production
        v2_feature_x  → feature_x

    Returns none for a non-V2 target.  Do not assume the environment is 'dev';
    it is whatever Neon branch the selected target maps to.
  -#}
  {% if is_v2_target() %}
    {{ return(target.name[3:]) }}
  {% else %}
    {{ return(none) }}
  {% endif %}
{% endmacro %}
