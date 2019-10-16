'''
Copyright (C) 2016 Centro de Computacao Cientifica e Software Livre
Departamento de Informatica - Universidade Federal do Parana - C3SL/UFPR

This file is part of HOTMapper.

HOTMapper is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

HOTMapper is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with HOTMapper.  If not, see <https://www.gnu.org/licenses/>.
'''

'''Group Settings'''

# ---------------------------------------------------------------------------------------#
# SMPPIR
# ---------------------------------------------------------------------------------------#
INEP = [
    'admission.sql',
    'course.sql',
    'evader.sql',
    'extracurricular_activities.sql',
    'graduate.sql',
    'institution.sql', 
    'institutionPrivate.sql',
    'social_support.sql',
    'student_loans.sql'
]
PROUNI = [
    'coursePROUNI.sql',
    'institutionPROUNI.sql',
    'prouni.sql'
]
PNAD = [
    'pnad.sql'
]
CADUNICO = [
    'eixo2.sql',
    'eixo3.sql',
    'eixo4.sql',
    'african_sustentability.sql',
    'african_rights.sql',
    'african_culture.sql'
]
FIES = [
    'courseFIES.sql',
    'fies.sql',
    'institutionFIES.sql'
]
ALL_GROUPS_SMPPIR = INEP + PROUNI + PNAD + CADUNICO + FIES
# ---------------------------------------------------------------------------------------#

# ---------------------------------------------------------------------------------------#
# SIMCAQ
# ---------------------------------------------------------------------------------------#
BASE = [
    'regiao.sql',
    'estado.sql',
    'municipio.sql',
    'siope_uf.sql',
    'siope_mun.sql',
    'siope_mun_seed.sql',
    'instituicao_superior.sql',
    'formacao_superior.sql',
    'formacao_superior_seed.sql',
    'ibge_pib.sql',
    'cub.sql',
]

SIMCAQ_AGGREGATE = [
    'docente_por_escola.sql',
    'idm.sql',
    'projecao_matricula.sql'
]
# ---------------------------------------------------------------------------------------#

# ---------------------------------------------------------------------------------------#
# Usado para chamar os grupos corretos
# ---------------------------------------------------------------------------------------#
DATA_GROUP = {
    "INEP": INEP,
    "PROUNI": PROUNI,
    "PNAD": PNAD,
    "CADUNICO": CADUNICO,
    "FIES": FIES,
    "ALL_GROUPS_SMPPIR": ALL_GROUPS_SMPPIR,
    "BASE": BASE,
    "SIMCAQ_AGGREGATE": SIMCAQ_AGGREGATE
}
# ---------------------------------------------------------------------------------------#
# Nome da tabela caso seja diferente do nome do sql
# ---------------------------------------------------------------------------------------#
DATABASE_TABLE_NAME = {
    'admission.sql': 'admission_ag',
    'course.sql': 'course_ag',
    'evader.sql': 'evader_ag',
    'extracurricular_activities.sql': 'extracurricular_activities_ag',
    'graduate.sql': 'graduate_ag',
    'institution.sql': 'institution_ag',
    'institutionPrivate.sql': 'institution_private_ag',
    'social_support.sql': 'social_support_ag',
    'student_loans.sql': 'student_loans_ag',
    'coursePROUNI.sql': 'course_prouni_ag',
    'institutionPROUNI.sql': 'institution_prouni_ag',
    'prouni.sql': 'prouni_ag',
    'eixo2.sql': 'quilombola_eixo_2_ag',
    'eixo3.sql': 'quilombola_eixo_3_ag',
    'eixo4.sql': 'quilombola_eixo_4_ag',
    'african_sustentability.sql': 'african_sustentability_ag',
    'african_rights.sql': 'african_rights_ag',
    'african_culture.sql': 'african_culture_ag',
    'pnad.sql': 'pnad_ag',
    'courseFIES.sql': 'course_fies_ag',
    'fies.sql': 'fies_ag',
    'institutionFIES.sql': 'institution_fies_ag',
    'idm.sql': 'indice_distribuicao_matriculas'
}
