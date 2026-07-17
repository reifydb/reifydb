// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	fmt::{Display, Formatter},
	ops::Deref,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ColumnId(pub u64);

impl ColumnId {
	pub const REQUEST_HISTORY_TIMESTAMP: ColumnId = ColumnId(1);
	pub const REQUEST_HISTORY_OPERATION: ColumnId = ColumnId(2);
	pub const REQUEST_HISTORY_FINGERPRINT: ColumnId = ColumnId(3);
	pub const REQUEST_HISTORY_TOTAL_DURATION: ColumnId = ColumnId(4);
	pub const REQUEST_HISTORY_COMPUTE_DURATION: ColumnId = ColumnId(5);
	pub const REQUEST_HISTORY_SUCCESS: ColumnId = ColumnId(6);
	pub const REQUEST_HISTORY_STATEMENT_COUNT: ColumnId = ColumnId(7);
	pub const REQUEST_HISTORY_NORMALIZED_RQL: ColumnId = ColumnId(8);

	pub const STATEMENT_STATS_SNAPSHOT_TIMESTAMP: ColumnId = ColumnId(9);
	pub const STATEMENT_STATS_FINGERPRINT: ColumnId = ColumnId(10);
	pub const STATEMENT_STATS_NORMALIZED_RQL: ColumnId = ColumnId(11);
	pub const STATEMENT_STATS_CALLS: ColumnId = ColumnId(12);
	pub const STATEMENT_STATS_TOTAL_DURATION: ColumnId = ColumnId(13);
	pub const STATEMENT_STATS_MEAN_DURATION: ColumnId = ColumnId(14);
	pub const STATEMENT_STATS_MAX_DURATION: ColumnId = ColumnId(15);
	pub const STATEMENT_STATS_MIN_DURATION: ColumnId = ColumnId(16);
	pub const STATEMENT_STATS_TOTAL_ROWS: ColumnId = ColumnId(17);
	pub const STATEMENT_STATS_ERRORS: ColumnId = ColumnId(18);

	pub const PROFILER_QUERY_SNAPSHOTS_TS: ColumnId = ColumnId(1024);
	pub const PROFILER_QUERY_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1025);
	pub const PROFILER_QUERY_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1026);
	pub const PROFILER_QUERY_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1027);
	pub const PROFILER_QUERY_SNAPSHOTS_CALLS: ColumnId = ColumnId(1028);
	pub const PROFILER_QUERY_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1029);
	pub const PROFILER_QUERY_SNAPSHOTS_MIN: ColumnId = ColumnId(1030);
	pub const PROFILER_QUERY_SNAPSHOTS_MAX: ColumnId = ColumnId(1031);
	pub const PROFILER_QUERY_SNAPSHOTS_P50: ColumnId = ColumnId(1032);
	pub const PROFILER_QUERY_SNAPSHOTS_P60: ColumnId = ColumnId(1033);
	pub const PROFILER_QUERY_SNAPSHOTS_P70: ColumnId = ColumnId(1034);
	pub const PROFILER_QUERY_SNAPSHOTS_P75: ColumnId = ColumnId(1035);
	pub const PROFILER_QUERY_SNAPSHOTS_P80: ColumnId = ColumnId(1036);
	pub const PROFILER_QUERY_SNAPSHOTS_P85: ColumnId = ColumnId(1037);
	pub const PROFILER_QUERY_SNAPSHOTS_P90: ColumnId = ColumnId(1038);
	pub const PROFILER_QUERY_SNAPSHOTS_P95: ColumnId = ColumnId(1039);
	pub const PROFILER_QUERY_SNAPSHOTS_P98: ColumnId = ColumnId(1040);
	pub const PROFILER_QUERY_SNAPSHOTS_P99: ColumnId = ColumnId(1041);
	pub const PROFILER_QUERY_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1042);
	pub const PROFILER_QUERY_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1043);
	pub const PROFILER_QUERY_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1044);
	pub const PROFILER_QUERY_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1045);
	pub const PROFILER_TXN_SNAPSHOTS_TS: ColumnId = ColumnId(1046);
	pub const PROFILER_TXN_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1047);
	pub const PROFILER_TXN_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1048);
	pub const PROFILER_TXN_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1049);
	pub const PROFILER_TXN_SNAPSHOTS_CALLS: ColumnId = ColumnId(1050);
	pub const PROFILER_TXN_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1051);
	pub const PROFILER_TXN_SNAPSHOTS_MIN: ColumnId = ColumnId(1052);
	pub const PROFILER_TXN_SNAPSHOTS_MAX: ColumnId = ColumnId(1053);
	pub const PROFILER_TXN_SNAPSHOTS_P50: ColumnId = ColumnId(1054);
	pub const PROFILER_TXN_SNAPSHOTS_P60: ColumnId = ColumnId(1055);
	pub const PROFILER_TXN_SNAPSHOTS_P70: ColumnId = ColumnId(1056);
	pub const PROFILER_TXN_SNAPSHOTS_P75: ColumnId = ColumnId(1057);
	pub const PROFILER_TXN_SNAPSHOTS_P80: ColumnId = ColumnId(1058);
	pub const PROFILER_TXN_SNAPSHOTS_P85: ColumnId = ColumnId(1059);
	pub const PROFILER_TXN_SNAPSHOTS_P90: ColumnId = ColumnId(1060);
	pub const PROFILER_TXN_SNAPSHOTS_P95: ColumnId = ColumnId(1061);
	pub const PROFILER_TXN_SNAPSHOTS_P98: ColumnId = ColumnId(1062);
	pub const PROFILER_TXN_SNAPSHOTS_P99: ColumnId = ColumnId(1063);
	pub const PROFILER_TXN_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1064);
	pub const PROFILER_TXN_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1065);
	pub const PROFILER_TXN_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1066);
	pub const PROFILER_TXN_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1067);
	pub const PROFILER_STORAGE_SNAPSHOTS_TS: ColumnId = ColumnId(1068);
	pub const PROFILER_STORAGE_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1069);
	pub const PROFILER_STORAGE_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1070);
	pub const PROFILER_STORAGE_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1071);
	pub const PROFILER_STORAGE_SNAPSHOTS_CALLS: ColumnId = ColumnId(1072);
	pub const PROFILER_STORAGE_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1073);
	pub const PROFILER_STORAGE_SNAPSHOTS_MIN: ColumnId = ColumnId(1074);
	pub const PROFILER_STORAGE_SNAPSHOTS_MAX: ColumnId = ColumnId(1075);
	pub const PROFILER_STORAGE_SNAPSHOTS_P50: ColumnId = ColumnId(1076);
	pub const PROFILER_STORAGE_SNAPSHOTS_P60: ColumnId = ColumnId(1077);
	pub const PROFILER_STORAGE_SNAPSHOTS_P70: ColumnId = ColumnId(1078);
	pub const PROFILER_STORAGE_SNAPSHOTS_P75: ColumnId = ColumnId(1079);
	pub const PROFILER_STORAGE_SNAPSHOTS_P80: ColumnId = ColumnId(1080);
	pub const PROFILER_STORAGE_SNAPSHOTS_P85: ColumnId = ColumnId(1081);
	pub const PROFILER_STORAGE_SNAPSHOTS_P90: ColumnId = ColumnId(1082);
	pub const PROFILER_STORAGE_SNAPSHOTS_P95: ColumnId = ColumnId(1083);
	pub const PROFILER_STORAGE_SNAPSHOTS_P98: ColumnId = ColumnId(1084);
	pub const PROFILER_STORAGE_SNAPSHOTS_P99: ColumnId = ColumnId(1085);
	pub const PROFILER_STORAGE_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1086);
	pub const PROFILER_STORAGE_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1087);
	pub const PROFILER_STORAGE_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1088);
	pub const PROFILER_STORAGE_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1089);
	pub const PROFILER_PLAN_SNAPSHOTS_TS: ColumnId = ColumnId(1090);
	pub const PROFILER_PLAN_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1091);
	pub const PROFILER_PLAN_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1092);
	pub const PROFILER_PLAN_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1093);
	pub const PROFILER_PLAN_SNAPSHOTS_CALLS: ColumnId = ColumnId(1094);
	pub const PROFILER_PLAN_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1095);
	pub const PROFILER_PLAN_SNAPSHOTS_MIN: ColumnId = ColumnId(1096);
	pub const PROFILER_PLAN_SNAPSHOTS_MAX: ColumnId = ColumnId(1097);
	pub const PROFILER_PLAN_SNAPSHOTS_P50: ColumnId = ColumnId(1098);
	pub const PROFILER_PLAN_SNAPSHOTS_P60: ColumnId = ColumnId(1099);
	pub const PROFILER_PLAN_SNAPSHOTS_P70: ColumnId = ColumnId(1100);
	pub const PROFILER_PLAN_SNAPSHOTS_P75: ColumnId = ColumnId(1101);
	pub const PROFILER_PLAN_SNAPSHOTS_P80: ColumnId = ColumnId(1102);
	pub const PROFILER_PLAN_SNAPSHOTS_P85: ColumnId = ColumnId(1103);
	pub const PROFILER_PLAN_SNAPSHOTS_P90: ColumnId = ColumnId(1104);
	pub const PROFILER_PLAN_SNAPSHOTS_P95: ColumnId = ColumnId(1105);
	pub const PROFILER_PLAN_SNAPSHOTS_P98: ColumnId = ColumnId(1106);
	pub const PROFILER_PLAN_SNAPSHOTS_P99: ColumnId = ColumnId(1107);
	pub const PROFILER_PLAN_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1108);
	pub const PROFILER_PLAN_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1109);
	pub const PROFILER_PLAN_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1110);
	pub const PROFILER_PLAN_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1111);
	pub const PROFILER_CDC_SNAPSHOTS_TS: ColumnId = ColumnId(1112);
	pub const PROFILER_CDC_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1113);
	pub const PROFILER_CDC_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1114);
	pub const PROFILER_CDC_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1115);
	pub const PROFILER_CDC_SNAPSHOTS_CALLS: ColumnId = ColumnId(1116);
	pub const PROFILER_CDC_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1117);
	pub const PROFILER_CDC_SNAPSHOTS_MIN: ColumnId = ColumnId(1118);
	pub const PROFILER_CDC_SNAPSHOTS_MAX: ColumnId = ColumnId(1119);
	pub const PROFILER_CDC_SNAPSHOTS_P50: ColumnId = ColumnId(1120);
	pub const PROFILER_CDC_SNAPSHOTS_P60: ColumnId = ColumnId(1121);
	pub const PROFILER_CDC_SNAPSHOTS_P70: ColumnId = ColumnId(1122);
	pub const PROFILER_CDC_SNAPSHOTS_P75: ColumnId = ColumnId(1123);
	pub const PROFILER_CDC_SNAPSHOTS_P80: ColumnId = ColumnId(1124);
	pub const PROFILER_CDC_SNAPSHOTS_P85: ColumnId = ColumnId(1125);
	pub const PROFILER_CDC_SNAPSHOTS_P90: ColumnId = ColumnId(1126);
	pub const PROFILER_CDC_SNAPSHOTS_P95: ColumnId = ColumnId(1127);
	pub const PROFILER_CDC_SNAPSHOTS_P98: ColumnId = ColumnId(1128);
	pub const PROFILER_CDC_SNAPSHOTS_P99: ColumnId = ColumnId(1129);
	pub const PROFILER_CDC_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1130);
	pub const PROFILER_CDC_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1131);
	pub const PROFILER_CDC_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1132);
	pub const PROFILER_CDC_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1133);
	pub const PROFILER_FLOW_SNAPSHOTS_TS: ColumnId = ColumnId(1134);
	pub const PROFILER_FLOW_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1135);
	pub const PROFILER_FLOW_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1136);
	pub const PROFILER_FLOW_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1137);
	pub const PROFILER_FLOW_SNAPSHOTS_CALLS: ColumnId = ColumnId(1138);
	pub const PROFILER_FLOW_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1139);
	pub const PROFILER_FLOW_SNAPSHOTS_MIN: ColumnId = ColumnId(1140);
	pub const PROFILER_FLOW_SNAPSHOTS_MAX: ColumnId = ColumnId(1141);
	pub const PROFILER_FLOW_SNAPSHOTS_P50: ColumnId = ColumnId(1142);
	pub const PROFILER_FLOW_SNAPSHOTS_P60: ColumnId = ColumnId(1143);
	pub const PROFILER_FLOW_SNAPSHOTS_P70: ColumnId = ColumnId(1144);
	pub const PROFILER_FLOW_SNAPSHOTS_P75: ColumnId = ColumnId(1145);
	pub const PROFILER_FLOW_SNAPSHOTS_P80: ColumnId = ColumnId(1146);
	pub const PROFILER_FLOW_SNAPSHOTS_P85: ColumnId = ColumnId(1147);
	pub const PROFILER_FLOW_SNAPSHOTS_P90: ColumnId = ColumnId(1148);
	pub const PROFILER_FLOW_SNAPSHOTS_P95: ColumnId = ColumnId(1149);
	pub const PROFILER_FLOW_SNAPSHOTS_P98: ColumnId = ColumnId(1150);
	pub const PROFILER_FLOW_SNAPSHOTS_P99: ColumnId = ColumnId(1151);
	pub const PROFILER_FLOW_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1152);
	pub const PROFILER_FLOW_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1153);
	pub const PROFILER_FLOW_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1154);
	pub const PROFILER_FLOW_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1155);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_TS: ColumnId = ColumnId(1156);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1157);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1158);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1159);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_CALLS: ColumnId = ColumnId(1160);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1161);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_MIN: ColumnId = ColumnId(1162);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_MAX: ColumnId = ColumnId(1163);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_P50: ColumnId = ColumnId(1164);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_P60: ColumnId = ColumnId(1165);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_P70: ColumnId = ColumnId(1166);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_P75: ColumnId = ColumnId(1167);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_P80: ColumnId = ColumnId(1168);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_P85: ColumnId = ColumnId(1169);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_P90: ColumnId = ColumnId(1170);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_P95: ColumnId = ColumnId(1171);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_P98: ColumnId = ColumnId(1172);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_P99: ColumnId = ColumnId(1173);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1174);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1175);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1176);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1177);
	pub const PROFILER_SERVER_SNAPSHOTS_TS: ColumnId = ColumnId(1178);
	pub const PROFILER_SERVER_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1179);
	pub const PROFILER_SERVER_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1180);
	pub const PROFILER_SERVER_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1181);
	pub const PROFILER_SERVER_SNAPSHOTS_CALLS: ColumnId = ColumnId(1182);
	pub const PROFILER_SERVER_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1183);
	pub const PROFILER_SERVER_SNAPSHOTS_MIN: ColumnId = ColumnId(1184);
	pub const PROFILER_SERVER_SNAPSHOTS_MAX: ColumnId = ColumnId(1185);
	pub const PROFILER_SERVER_SNAPSHOTS_P50: ColumnId = ColumnId(1186);
	pub const PROFILER_SERVER_SNAPSHOTS_P60: ColumnId = ColumnId(1187);
	pub const PROFILER_SERVER_SNAPSHOTS_P70: ColumnId = ColumnId(1188);
	pub const PROFILER_SERVER_SNAPSHOTS_P75: ColumnId = ColumnId(1189);
	pub const PROFILER_SERVER_SNAPSHOTS_P80: ColumnId = ColumnId(1190);
	pub const PROFILER_SERVER_SNAPSHOTS_P85: ColumnId = ColumnId(1191);
	pub const PROFILER_SERVER_SNAPSHOTS_P90: ColumnId = ColumnId(1192);
	pub const PROFILER_SERVER_SNAPSHOTS_P95: ColumnId = ColumnId(1193);
	pub const PROFILER_SERVER_SNAPSHOTS_P98: ColumnId = ColumnId(1194);
	pub const PROFILER_SERVER_SNAPSHOTS_P99: ColumnId = ColumnId(1195);
	pub const PROFILER_SERVER_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1196);
	pub const PROFILER_SERVER_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1197);
	pub const PROFILER_SERVER_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1198);
	pub const PROFILER_SERVER_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1199);
	pub const PROFILER_WIRE_SNAPSHOTS_TS: ColumnId = ColumnId(1200);
	pub const PROFILER_WIRE_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1201);
	pub const PROFILER_WIRE_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1202);
	pub const PROFILER_WIRE_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1203);
	pub const PROFILER_WIRE_SNAPSHOTS_CALLS: ColumnId = ColumnId(1204);
	pub const PROFILER_WIRE_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1205);
	pub const PROFILER_WIRE_SNAPSHOTS_MIN: ColumnId = ColumnId(1206);
	pub const PROFILER_WIRE_SNAPSHOTS_MAX: ColumnId = ColumnId(1207);
	pub const PROFILER_WIRE_SNAPSHOTS_P50: ColumnId = ColumnId(1208);
	pub const PROFILER_WIRE_SNAPSHOTS_P60: ColumnId = ColumnId(1209);
	pub const PROFILER_WIRE_SNAPSHOTS_P70: ColumnId = ColumnId(1210);
	pub const PROFILER_WIRE_SNAPSHOTS_P75: ColumnId = ColumnId(1211);
	pub const PROFILER_WIRE_SNAPSHOTS_P80: ColumnId = ColumnId(1212);
	pub const PROFILER_WIRE_SNAPSHOTS_P85: ColumnId = ColumnId(1213);
	pub const PROFILER_WIRE_SNAPSHOTS_P90: ColumnId = ColumnId(1214);
	pub const PROFILER_WIRE_SNAPSHOTS_P95: ColumnId = ColumnId(1215);
	pub const PROFILER_WIRE_SNAPSHOTS_P98: ColumnId = ColumnId(1216);
	pub const PROFILER_WIRE_SNAPSHOTS_P99: ColumnId = ColumnId(1217);
	pub const PROFILER_WIRE_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1218);
	pub const PROFILER_WIRE_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1219);
	pub const PROFILER_WIRE_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1220);
	pub const PROFILER_WIRE_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1221);
	pub const PROFILER_AUTH_SNAPSHOTS_TS: ColumnId = ColumnId(1222);
	pub const PROFILER_AUTH_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1223);
	pub const PROFILER_AUTH_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1224);
	pub const PROFILER_AUTH_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1225);
	pub const PROFILER_AUTH_SNAPSHOTS_CALLS: ColumnId = ColumnId(1226);
	pub const PROFILER_AUTH_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1227);
	pub const PROFILER_AUTH_SNAPSHOTS_MIN: ColumnId = ColumnId(1228);
	pub const PROFILER_AUTH_SNAPSHOTS_MAX: ColumnId = ColumnId(1229);
	pub const PROFILER_AUTH_SNAPSHOTS_P50: ColumnId = ColumnId(1230);
	pub const PROFILER_AUTH_SNAPSHOTS_P60: ColumnId = ColumnId(1231);
	pub const PROFILER_AUTH_SNAPSHOTS_P70: ColumnId = ColumnId(1232);
	pub const PROFILER_AUTH_SNAPSHOTS_P75: ColumnId = ColumnId(1233);
	pub const PROFILER_AUTH_SNAPSHOTS_P80: ColumnId = ColumnId(1234);
	pub const PROFILER_AUTH_SNAPSHOTS_P85: ColumnId = ColumnId(1235);
	pub const PROFILER_AUTH_SNAPSHOTS_P90: ColumnId = ColumnId(1236);
	pub const PROFILER_AUTH_SNAPSHOTS_P95: ColumnId = ColumnId(1237);
	pub const PROFILER_AUTH_SNAPSHOTS_P98: ColumnId = ColumnId(1238);
	pub const PROFILER_AUTH_SNAPSHOTS_P99: ColumnId = ColumnId(1239);
	pub const PROFILER_AUTH_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1240);
	pub const PROFILER_AUTH_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1241);
	pub const PROFILER_AUTH_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1242);
	pub const PROFILER_AUTH_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1243);
	pub const PROFILER_CATALOG_SNAPSHOTS_TS: ColumnId = ColumnId(1244);
	pub const PROFILER_CATALOG_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1245);
	pub const PROFILER_CATALOG_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1246);
	pub const PROFILER_CATALOG_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1247);
	pub const PROFILER_CATALOG_SNAPSHOTS_CALLS: ColumnId = ColumnId(1248);
	pub const PROFILER_CATALOG_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1249);
	pub const PROFILER_CATALOG_SNAPSHOTS_MIN: ColumnId = ColumnId(1250);
	pub const PROFILER_CATALOG_SNAPSHOTS_MAX: ColumnId = ColumnId(1251);
	pub const PROFILER_CATALOG_SNAPSHOTS_P50: ColumnId = ColumnId(1252);
	pub const PROFILER_CATALOG_SNAPSHOTS_P60: ColumnId = ColumnId(1253);
	pub const PROFILER_CATALOG_SNAPSHOTS_P70: ColumnId = ColumnId(1254);
	pub const PROFILER_CATALOG_SNAPSHOTS_P75: ColumnId = ColumnId(1255);
	pub const PROFILER_CATALOG_SNAPSHOTS_P80: ColumnId = ColumnId(1256);
	pub const PROFILER_CATALOG_SNAPSHOTS_P85: ColumnId = ColumnId(1257);
	pub const PROFILER_CATALOG_SNAPSHOTS_P90: ColumnId = ColumnId(1258);
	pub const PROFILER_CATALOG_SNAPSHOTS_P95: ColumnId = ColumnId(1259);
	pub const PROFILER_CATALOG_SNAPSHOTS_P98: ColumnId = ColumnId(1260);
	pub const PROFILER_CATALOG_SNAPSHOTS_P99: ColumnId = ColumnId(1261);
	pub const PROFILER_CATALOG_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1262);
	pub const PROFILER_CATALOG_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1263);
	pub const PROFILER_CATALOG_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1264);
	pub const PROFILER_CATALOG_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1265);
	pub const PROFILER_ENGINE_SNAPSHOTS_TS: ColumnId = ColumnId(1266);
	pub const PROFILER_ENGINE_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1267);
	pub const PROFILER_ENGINE_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1268);
	pub const PROFILER_ENGINE_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1269);
	pub const PROFILER_ENGINE_SNAPSHOTS_CALLS: ColumnId = ColumnId(1270);
	pub const PROFILER_ENGINE_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1271);
	pub const PROFILER_ENGINE_SNAPSHOTS_MIN: ColumnId = ColumnId(1272);
	pub const PROFILER_ENGINE_SNAPSHOTS_MAX: ColumnId = ColumnId(1273);
	pub const PROFILER_ENGINE_SNAPSHOTS_P50: ColumnId = ColumnId(1274);
	pub const PROFILER_ENGINE_SNAPSHOTS_P60: ColumnId = ColumnId(1275);
	pub const PROFILER_ENGINE_SNAPSHOTS_P70: ColumnId = ColumnId(1276);
	pub const PROFILER_ENGINE_SNAPSHOTS_P75: ColumnId = ColumnId(1277);
	pub const PROFILER_ENGINE_SNAPSHOTS_P80: ColumnId = ColumnId(1278);
	pub const PROFILER_ENGINE_SNAPSHOTS_P85: ColumnId = ColumnId(1279);
	pub const PROFILER_ENGINE_SNAPSHOTS_P90: ColumnId = ColumnId(1280);
	pub const PROFILER_ENGINE_SNAPSHOTS_P95: ColumnId = ColumnId(1281);
	pub const PROFILER_ENGINE_SNAPSHOTS_P98: ColumnId = ColumnId(1282);
	pub const PROFILER_ENGINE_SNAPSHOTS_P99: ColumnId = ColumnId(1283);
	pub const PROFILER_ENGINE_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1284);
	pub const PROFILER_ENGINE_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1285);
	pub const PROFILER_ENGINE_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1286);
	pub const PROFILER_ENGINE_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1287);
	pub const PROFILER_MUTATE_SNAPSHOTS_TS: ColumnId = ColumnId(1288);
	pub const PROFILER_MUTATE_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1289);
	pub const PROFILER_MUTATE_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1290);
	pub const PROFILER_MUTATE_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1291);
	pub const PROFILER_MUTATE_SNAPSHOTS_CALLS: ColumnId = ColumnId(1292);
	pub const PROFILER_MUTATE_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1293);
	pub const PROFILER_MUTATE_SNAPSHOTS_MIN: ColumnId = ColumnId(1294);
	pub const PROFILER_MUTATE_SNAPSHOTS_MAX: ColumnId = ColumnId(1295);
	pub const PROFILER_MUTATE_SNAPSHOTS_P50: ColumnId = ColumnId(1296);
	pub const PROFILER_MUTATE_SNAPSHOTS_P60: ColumnId = ColumnId(1297);
	pub const PROFILER_MUTATE_SNAPSHOTS_P70: ColumnId = ColumnId(1298);
	pub const PROFILER_MUTATE_SNAPSHOTS_P75: ColumnId = ColumnId(1299);
	pub const PROFILER_MUTATE_SNAPSHOTS_P80: ColumnId = ColumnId(1300);
	pub const PROFILER_MUTATE_SNAPSHOTS_P85: ColumnId = ColumnId(1301);
	pub const PROFILER_MUTATE_SNAPSHOTS_P90: ColumnId = ColumnId(1302);
	pub const PROFILER_MUTATE_SNAPSHOTS_P95: ColumnId = ColumnId(1303);
	pub const PROFILER_MUTATE_SNAPSHOTS_P98: ColumnId = ColumnId(1304);
	pub const PROFILER_MUTATE_SNAPSHOTS_P99: ColumnId = ColumnId(1305);
	pub const PROFILER_MUTATE_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1306);
	pub const PROFILER_MUTATE_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1307);
	pub const PROFILER_MUTATE_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1308);
	pub const PROFILER_MUTATE_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1309);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_TS: ColumnId = ColumnId(1310);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1311);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1312);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1313);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_CALLS: ColumnId = ColumnId(1314);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1315);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_MIN: ColumnId = ColumnId(1316);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_MAX: ColumnId = ColumnId(1317);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_P50: ColumnId = ColumnId(1318);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_P60: ColumnId = ColumnId(1319);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_P70: ColumnId = ColumnId(1320);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_P75: ColumnId = ColumnId(1321);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_P80: ColumnId = ColumnId(1322);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_P85: ColumnId = ColumnId(1323);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_P90: ColumnId = ColumnId(1324);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_P95: ColumnId = ColumnId(1325);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_P98: ColumnId = ColumnId(1326);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_P99: ColumnId = ColumnId(1327);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1328);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1329);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1330);
	pub const PROFILER_TRANSPORT_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1331);
	pub const PROFILER_TASK_SNAPSHOTS_TS: ColumnId = ColumnId(1332);
	pub const PROFILER_TASK_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1333);
	pub const PROFILER_TASK_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1334);
	pub const PROFILER_TASK_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1335);
	pub const PROFILER_TASK_SNAPSHOTS_CALLS: ColumnId = ColumnId(1336);
	pub const PROFILER_TASK_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1337);
	pub const PROFILER_TASK_SNAPSHOTS_MIN: ColumnId = ColumnId(1338);
	pub const PROFILER_TASK_SNAPSHOTS_MAX: ColumnId = ColumnId(1339);
	pub const PROFILER_TASK_SNAPSHOTS_P50: ColumnId = ColumnId(1340);
	pub const PROFILER_TASK_SNAPSHOTS_P60: ColumnId = ColumnId(1341);
	pub const PROFILER_TASK_SNAPSHOTS_P70: ColumnId = ColumnId(1342);
	pub const PROFILER_TASK_SNAPSHOTS_P75: ColumnId = ColumnId(1343);
	pub const PROFILER_TASK_SNAPSHOTS_P80: ColumnId = ColumnId(1344);
	pub const PROFILER_TASK_SNAPSHOTS_P85: ColumnId = ColumnId(1345);
	pub const PROFILER_TASK_SNAPSHOTS_P90: ColumnId = ColumnId(1346);
	pub const PROFILER_TASK_SNAPSHOTS_P95: ColumnId = ColumnId(1347);
	pub const PROFILER_TASK_SNAPSHOTS_P98: ColumnId = ColumnId(1348);
	pub const PROFILER_TASK_SNAPSHOTS_P99: ColumnId = ColumnId(1349);
	pub const PROFILER_TASK_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1350);
	pub const PROFILER_TASK_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1351);
	pub const PROFILER_TASK_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1352);
	pub const PROFILER_TASK_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1353);
	pub const PROFILER_POLICY_SNAPSHOTS_TS: ColumnId = ColumnId(1354);
	pub const PROFILER_POLICY_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1355);
	pub const PROFILER_POLICY_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1356);
	pub const PROFILER_POLICY_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1357);
	pub const PROFILER_POLICY_SNAPSHOTS_CALLS: ColumnId = ColumnId(1358);
	pub const PROFILER_POLICY_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1359);
	pub const PROFILER_POLICY_SNAPSHOTS_MIN: ColumnId = ColumnId(1360);
	pub const PROFILER_POLICY_SNAPSHOTS_MAX: ColumnId = ColumnId(1361);
	pub const PROFILER_POLICY_SNAPSHOTS_P50: ColumnId = ColumnId(1362);
	pub const PROFILER_POLICY_SNAPSHOTS_P60: ColumnId = ColumnId(1363);
	pub const PROFILER_POLICY_SNAPSHOTS_P70: ColumnId = ColumnId(1364);
	pub const PROFILER_POLICY_SNAPSHOTS_P75: ColumnId = ColumnId(1365);
	pub const PROFILER_POLICY_SNAPSHOTS_P80: ColumnId = ColumnId(1366);
	pub const PROFILER_POLICY_SNAPSHOTS_P85: ColumnId = ColumnId(1367);
	pub const PROFILER_POLICY_SNAPSHOTS_P90: ColumnId = ColumnId(1368);
	pub const PROFILER_POLICY_SNAPSHOTS_P95: ColumnId = ColumnId(1369);
	pub const PROFILER_POLICY_SNAPSHOTS_P98: ColumnId = ColumnId(1370);
	pub const PROFILER_POLICY_SNAPSHOTS_P99: ColumnId = ColumnId(1371);
	pub const PROFILER_POLICY_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1372);
	pub const PROFILER_POLICY_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1373);
	pub const PROFILER_POLICY_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1374);
	pub const PROFILER_POLICY_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1375);
	pub const PROFILER_FFI_SNAPSHOTS_TS: ColumnId = ColumnId(1376);
	pub const PROFILER_FFI_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1377);
	pub const PROFILER_FFI_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1378);
	pub const PROFILER_FFI_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1379);
	pub const PROFILER_FFI_SNAPSHOTS_CALLS: ColumnId = ColumnId(1380);
	pub const PROFILER_FFI_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1381);
	pub const PROFILER_FFI_SNAPSHOTS_MIN: ColumnId = ColumnId(1382);
	pub const PROFILER_FFI_SNAPSHOTS_MAX: ColumnId = ColumnId(1383);
	pub const PROFILER_FFI_SNAPSHOTS_P50: ColumnId = ColumnId(1384);
	pub const PROFILER_FFI_SNAPSHOTS_P60: ColumnId = ColumnId(1385);
	pub const PROFILER_FFI_SNAPSHOTS_P70: ColumnId = ColumnId(1386);
	pub const PROFILER_FFI_SNAPSHOTS_P75: ColumnId = ColumnId(1387);
	pub const PROFILER_FFI_SNAPSHOTS_P80: ColumnId = ColumnId(1388);
	pub const PROFILER_FFI_SNAPSHOTS_P85: ColumnId = ColumnId(1389);
	pub const PROFILER_FFI_SNAPSHOTS_P90: ColumnId = ColumnId(1390);
	pub const PROFILER_FFI_SNAPSHOTS_P95: ColumnId = ColumnId(1391);
	pub const PROFILER_FFI_SNAPSHOTS_P98: ColumnId = ColumnId(1392);
	pub const PROFILER_FFI_SNAPSHOTS_P99: ColumnId = ColumnId(1393);
	pub const PROFILER_FFI_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1394);
	pub const PROFILER_FFI_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1395);
	pub const PROFILER_FFI_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1396);
	pub const PROFILER_FFI_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1397);
	pub const RUNTIME_MEMORY_SNAPSHOTS_TS: ColumnId = ColumnId(1398);
	pub const RUNTIME_MEMORY_SNAPSHOTS_SCOPE: ColumnId = ColumnId(1399);
	pub const RUNTIME_MEMORY_SNAPSHOTS_METRIC: ColumnId = ColumnId(1400);
	pub const RUNTIME_MEMORY_SNAPSHOTS_VALUE: ColumnId = ColumnId(1401);
	pub const RUNTIME_MEMORY_SNAPSHOTS_UNIT: ColumnId = ColumnId(1402);
	pub const RUNTIME_WATERMARKS_SNAPSHOTS_TS: ColumnId = ColumnId(1403);
	pub const RUNTIME_WATERMARKS_SNAPSHOTS_SCOPE: ColumnId = ColumnId(1404);
	pub const RUNTIME_WATERMARKS_SNAPSHOTS_METRIC: ColumnId = ColumnId(1405);
	pub const RUNTIME_WATERMARKS_SNAPSHOTS_VALUE: ColumnId = ColumnId(1406);
	pub const RUNTIME_WATERMARKS_SNAPSHOTS_UNIT: ColumnId = ColumnId(1407);

	pub const PROFILER_CACHE_SNAPSHOTS_TS: ColumnId = ColumnId(1408);
	pub const PROFILER_CACHE_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1409);
	pub const PROFILER_CACHE_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1410);
	pub const PROFILER_CACHE_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1411);
	pub const PROFILER_CACHE_SNAPSHOTS_CALLS: ColumnId = ColumnId(1412);
	pub const PROFILER_CACHE_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1413);
	pub const PROFILER_CACHE_SNAPSHOTS_MIN: ColumnId = ColumnId(1414);
	pub const PROFILER_CACHE_SNAPSHOTS_MAX: ColumnId = ColumnId(1415);
	pub const PROFILER_CACHE_SNAPSHOTS_P50: ColumnId = ColumnId(1416);
	pub const PROFILER_CACHE_SNAPSHOTS_P60: ColumnId = ColumnId(1417);
	pub const PROFILER_CACHE_SNAPSHOTS_P70: ColumnId = ColumnId(1418);
	pub const PROFILER_CACHE_SNAPSHOTS_P75: ColumnId = ColumnId(1419);
	pub const PROFILER_CACHE_SNAPSHOTS_P80: ColumnId = ColumnId(1420);
	pub const PROFILER_CACHE_SNAPSHOTS_P85: ColumnId = ColumnId(1421);
	pub const PROFILER_CACHE_SNAPSHOTS_P90: ColumnId = ColumnId(1422);
	pub const PROFILER_CACHE_SNAPSHOTS_P95: ColumnId = ColumnId(1423);
	pub const PROFILER_CACHE_SNAPSHOTS_P98: ColumnId = ColumnId(1424);
	pub const PROFILER_CACHE_SNAPSHOTS_P99: ColumnId = ColumnId(1425);
	pub const PROFILER_CACHE_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1426);
	pub const PROFILER_CACHE_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1427);
	pub const PROFILER_CACHE_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1428);
	pub const PROFILER_CACHE_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1429);
	pub const PROFILER_SHAPE_SNAPSHOTS_TS: ColumnId = ColumnId(1430);
	pub const PROFILER_SHAPE_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1431);
	pub const PROFILER_SHAPE_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1432);
	pub const PROFILER_SHAPE_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1433);
	pub const PROFILER_SHAPE_SNAPSHOTS_CALLS: ColumnId = ColumnId(1434);
	pub const PROFILER_SHAPE_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1435);
	pub const PROFILER_SHAPE_SNAPSHOTS_MIN: ColumnId = ColumnId(1436);
	pub const PROFILER_SHAPE_SNAPSHOTS_MAX: ColumnId = ColumnId(1437);
	pub const PROFILER_SHAPE_SNAPSHOTS_P50: ColumnId = ColumnId(1438);
	pub const PROFILER_SHAPE_SNAPSHOTS_P60: ColumnId = ColumnId(1439);
	pub const PROFILER_SHAPE_SNAPSHOTS_P70: ColumnId = ColumnId(1440);
	pub const PROFILER_SHAPE_SNAPSHOTS_P75: ColumnId = ColumnId(1441);
	pub const PROFILER_SHAPE_SNAPSHOTS_P80: ColumnId = ColumnId(1442);
	pub const PROFILER_SHAPE_SNAPSHOTS_P85: ColumnId = ColumnId(1443);
	pub const PROFILER_SHAPE_SNAPSHOTS_P90: ColumnId = ColumnId(1444);
	pub const PROFILER_SHAPE_SNAPSHOTS_P95: ColumnId = ColumnId(1445);
	pub const PROFILER_SHAPE_SNAPSHOTS_P98: ColumnId = ColumnId(1446);
	pub const PROFILER_SHAPE_SNAPSHOTS_P99: ColumnId = ColumnId(1447);
	pub const PROFILER_SHAPE_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1448);
	pub const PROFILER_SHAPE_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1449);
	pub const PROFILER_SHAPE_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1450);
	pub const PROFILER_SHAPE_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1451);
	pub const PROFILER_API_SNAPSHOTS_TS: ColumnId = ColumnId(1452);
	pub const PROFILER_API_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1453);
	pub const PROFILER_API_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1454);
	pub const PROFILER_API_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1455);
	pub const PROFILER_API_SNAPSHOTS_CALLS: ColumnId = ColumnId(1456);
	pub const PROFILER_API_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1457);
	pub const PROFILER_API_SNAPSHOTS_MIN: ColumnId = ColumnId(1458);
	pub const PROFILER_API_SNAPSHOTS_MAX: ColumnId = ColumnId(1459);
	pub const PROFILER_API_SNAPSHOTS_P50: ColumnId = ColumnId(1460);
	pub const PROFILER_API_SNAPSHOTS_P60: ColumnId = ColumnId(1461);
	pub const PROFILER_API_SNAPSHOTS_P70: ColumnId = ColumnId(1462);
	pub const PROFILER_API_SNAPSHOTS_P75: ColumnId = ColumnId(1463);
	pub const PROFILER_API_SNAPSHOTS_P80: ColumnId = ColumnId(1464);
	pub const PROFILER_API_SNAPSHOTS_P85: ColumnId = ColumnId(1465);
	pub const PROFILER_API_SNAPSHOTS_P90: ColumnId = ColumnId(1466);
	pub const PROFILER_API_SNAPSHOTS_P95: ColumnId = ColumnId(1467);
	pub const PROFILER_API_SNAPSHOTS_P98: ColumnId = ColumnId(1468);
	pub const PROFILER_API_SNAPSHOTS_P99: ColumnId = ColumnId(1469);
	pub const PROFILER_API_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1470);
	pub const PROFILER_API_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1471);
	pub const PROFILER_API_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1472);
	pub const PROFILER_API_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1473);
	pub const PROFILER_ACTOR_SNAPSHOTS_TS: ColumnId = ColumnId(1474);
	pub const PROFILER_ACTOR_SNAPSHOTS_SPAN_NAME: ColumnId = ColumnId(1475);
	pub const PROFILER_ACTOR_SNAPSHOTS_DIM_1: ColumnId = ColumnId(1476);
	pub const PROFILER_ACTOR_SNAPSHOTS_DIM_2: ColumnId = ColumnId(1477);
	pub const PROFILER_ACTOR_SNAPSHOTS_CALLS: ColumnId = ColumnId(1478);
	pub const PROFILER_ACTOR_SNAPSHOTS_TOTAL: ColumnId = ColumnId(1479);
	pub const PROFILER_ACTOR_SNAPSHOTS_MIN: ColumnId = ColumnId(1480);
	pub const PROFILER_ACTOR_SNAPSHOTS_MAX: ColumnId = ColumnId(1481);
	pub const PROFILER_ACTOR_SNAPSHOTS_P50: ColumnId = ColumnId(1482);
	pub const PROFILER_ACTOR_SNAPSHOTS_P60: ColumnId = ColumnId(1483);
	pub const PROFILER_ACTOR_SNAPSHOTS_P70: ColumnId = ColumnId(1484);
	pub const PROFILER_ACTOR_SNAPSHOTS_P75: ColumnId = ColumnId(1485);
	pub const PROFILER_ACTOR_SNAPSHOTS_P80: ColumnId = ColumnId(1486);
	pub const PROFILER_ACTOR_SNAPSHOTS_P85: ColumnId = ColumnId(1487);
	pub const PROFILER_ACTOR_SNAPSHOTS_P90: ColumnId = ColumnId(1488);
	pub const PROFILER_ACTOR_SNAPSHOTS_P95: ColumnId = ColumnId(1489);
	pub const PROFILER_ACTOR_SNAPSHOTS_P98: ColumnId = ColumnId(1490);
	pub const PROFILER_ACTOR_SNAPSHOTS_P99: ColumnId = ColumnId(1491);
	pub const PROFILER_ACTOR_SNAPSHOTS_EXTRA_0: ColumnId = ColumnId(1492);
	pub const PROFILER_ACTOR_SNAPSHOTS_EXTRA_1: ColumnId = ColumnId(1493);
	pub const PROFILER_ACTOR_SNAPSHOTS_EXTRA_2: ColumnId = ColumnId(1494);
	pub const PROFILER_ACTOR_SNAPSHOTS_EXTRA_3: ColumnId = ColumnId(1495);

	pub const RUNTIME_OPERATORS_SNAPSHOTS_TS: ColumnId = ColumnId(1496);
	pub const RUNTIME_OPERATORS_SNAPSHOTS_SCOPE: ColumnId = ColumnId(1497);
	pub const RUNTIME_OPERATORS_SNAPSHOTS_METRIC: ColumnId = ColumnId(1498);
	pub const RUNTIME_OPERATORS_SNAPSHOTS_VALUE: ColumnId = ColumnId(1499);
	pub const RUNTIME_OPERATORS_SNAPSHOTS_UNIT: ColumnId = ColumnId(1500);
	pub const STORAGE_TABLE_SNAPSHOTS_TS: ColumnId = ColumnId(1501);
	pub const STORAGE_TABLE_SNAPSHOTS_ID: ColumnId = ColumnId(1502);
	pub const STORAGE_TABLE_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1503);
	pub const STORAGE_TABLE_SNAPSHOTS_TIER: ColumnId = ColumnId(1504);
	pub const STORAGE_TABLE_SNAPSHOTS_CURRENT_KEY_BYTES: ColumnId = ColumnId(1505);
	pub const STORAGE_TABLE_SNAPSHOTS_CURRENT_VALUE_BYTES: ColumnId = ColumnId(1506);
	pub const STORAGE_TABLE_SNAPSHOTS_CURRENT_TOTAL_BYTES: ColumnId = ColumnId(1507);
	pub const STORAGE_TABLE_SNAPSHOTS_CURRENT_COUNT: ColumnId = ColumnId(1508);
	pub const STORAGE_TABLE_SNAPSHOTS_HISTORICAL_KEY_BYTES: ColumnId = ColumnId(1509);
	pub const STORAGE_TABLE_SNAPSHOTS_HISTORICAL_VALUE_BYTES: ColumnId = ColumnId(1510);
	pub const STORAGE_TABLE_SNAPSHOTS_HISTORICAL_TOTAL_BYTES: ColumnId = ColumnId(1511);
	pub const STORAGE_TABLE_SNAPSHOTS_HISTORICAL_COUNT: ColumnId = ColumnId(1512);
	pub const STORAGE_TABLE_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1513);
	pub const STORAGE_VIEW_SNAPSHOTS_TS: ColumnId = ColumnId(1514);
	pub const STORAGE_VIEW_SNAPSHOTS_ID: ColumnId = ColumnId(1515);
	pub const STORAGE_VIEW_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1516);
	pub const STORAGE_VIEW_SNAPSHOTS_TIER: ColumnId = ColumnId(1517);
	pub const STORAGE_VIEW_SNAPSHOTS_CURRENT_KEY_BYTES: ColumnId = ColumnId(1518);
	pub const STORAGE_VIEW_SNAPSHOTS_CURRENT_VALUE_BYTES: ColumnId = ColumnId(1519);
	pub const STORAGE_VIEW_SNAPSHOTS_CURRENT_TOTAL_BYTES: ColumnId = ColumnId(1520);
	pub const STORAGE_VIEW_SNAPSHOTS_CURRENT_COUNT: ColumnId = ColumnId(1521);
	pub const STORAGE_VIEW_SNAPSHOTS_HISTORICAL_KEY_BYTES: ColumnId = ColumnId(1522);
	pub const STORAGE_VIEW_SNAPSHOTS_HISTORICAL_VALUE_BYTES: ColumnId = ColumnId(1523);
	pub const STORAGE_VIEW_SNAPSHOTS_HISTORICAL_TOTAL_BYTES: ColumnId = ColumnId(1524);
	pub const STORAGE_VIEW_SNAPSHOTS_HISTORICAL_COUNT: ColumnId = ColumnId(1525);
	pub const STORAGE_VIEW_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1526);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_TS: ColumnId = ColumnId(1527);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_ID: ColumnId = ColumnId(1528);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1529);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_TIER: ColumnId = ColumnId(1530);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_CURRENT_KEY_BYTES: ColumnId = ColumnId(1531);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_CURRENT_VALUE_BYTES: ColumnId = ColumnId(1532);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_CURRENT_TOTAL_BYTES: ColumnId = ColumnId(1533);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_CURRENT_COUNT: ColumnId = ColumnId(1534);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_HISTORICAL_KEY_BYTES: ColumnId = ColumnId(1535);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_HISTORICAL_VALUE_BYTES: ColumnId = ColumnId(1536);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_HISTORICAL_TOTAL_BYTES: ColumnId = ColumnId(1537);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_HISTORICAL_COUNT: ColumnId = ColumnId(1538);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1539);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_TS: ColumnId = ColumnId(1540);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_ID: ColumnId = ColumnId(1541);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1542);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_TIER: ColumnId = ColumnId(1543);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_CURRENT_KEY_BYTES: ColumnId = ColumnId(1544);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_CURRENT_VALUE_BYTES: ColumnId = ColumnId(1545);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_CURRENT_TOTAL_BYTES: ColumnId = ColumnId(1546);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_CURRENT_COUNT: ColumnId = ColumnId(1547);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_HISTORICAL_KEY_BYTES: ColumnId = ColumnId(1548);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_HISTORICAL_VALUE_BYTES: ColumnId = ColumnId(1549);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_HISTORICAL_TOTAL_BYTES: ColumnId = ColumnId(1550);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_HISTORICAL_COUNT: ColumnId = ColumnId(1551);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1552);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_TS: ColumnId = ColumnId(1553);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_ID: ColumnId = ColumnId(1554);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1555);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_TIER: ColumnId = ColumnId(1556);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_CURRENT_KEY_BYTES: ColumnId = ColumnId(1557);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_CURRENT_VALUE_BYTES: ColumnId = ColumnId(1558);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_CURRENT_TOTAL_BYTES: ColumnId = ColumnId(1559);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_CURRENT_COUNT: ColumnId = ColumnId(1560);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_HISTORICAL_KEY_BYTES: ColumnId = ColumnId(1561);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_HISTORICAL_VALUE_BYTES: ColumnId = ColumnId(1562);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_HISTORICAL_TOTAL_BYTES: ColumnId = ColumnId(1563);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_HISTORICAL_COUNT: ColumnId = ColumnId(1564);
	pub const STORAGE_DICTIONARY_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1565);
	pub const STORAGE_SERIES_SNAPSHOTS_TS: ColumnId = ColumnId(1566);
	pub const STORAGE_SERIES_SNAPSHOTS_ID: ColumnId = ColumnId(1567);
	pub const STORAGE_SERIES_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1568);
	pub const STORAGE_SERIES_SNAPSHOTS_TIER: ColumnId = ColumnId(1569);
	pub const STORAGE_SERIES_SNAPSHOTS_CURRENT_KEY_BYTES: ColumnId = ColumnId(1570);
	pub const STORAGE_SERIES_SNAPSHOTS_CURRENT_VALUE_BYTES: ColumnId = ColumnId(1571);
	pub const STORAGE_SERIES_SNAPSHOTS_CURRENT_TOTAL_BYTES: ColumnId = ColumnId(1572);
	pub const STORAGE_SERIES_SNAPSHOTS_CURRENT_COUNT: ColumnId = ColumnId(1573);
	pub const STORAGE_SERIES_SNAPSHOTS_HISTORICAL_KEY_BYTES: ColumnId = ColumnId(1574);
	pub const STORAGE_SERIES_SNAPSHOTS_HISTORICAL_VALUE_BYTES: ColumnId = ColumnId(1575);
	pub const STORAGE_SERIES_SNAPSHOTS_HISTORICAL_TOTAL_BYTES: ColumnId = ColumnId(1576);
	pub const STORAGE_SERIES_SNAPSHOTS_HISTORICAL_COUNT: ColumnId = ColumnId(1577);
	pub const STORAGE_SERIES_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1578);
	pub const STORAGE_FLOW_SNAPSHOTS_TS: ColumnId = ColumnId(1579);
	pub const STORAGE_FLOW_SNAPSHOTS_ID: ColumnId = ColumnId(1580);
	pub const STORAGE_FLOW_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1581);
	pub const STORAGE_FLOW_SNAPSHOTS_TIER: ColumnId = ColumnId(1582);
	pub const STORAGE_FLOW_SNAPSHOTS_CURRENT_KEY_BYTES: ColumnId = ColumnId(1583);
	pub const STORAGE_FLOW_SNAPSHOTS_CURRENT_VALUE_BYTES: ColumnId = ColumnId(1584);
	pub const STORAGE_FLOW_SNAPSHOTS_CURRENT_TOTAL_BYTES: ColumnId = ColumnId(1585);
	pub const STORAGE_FLOW_SNAPSHOTS_CURRENT_COUNT: ColumnId = ColumnId(1586);
	pub const STORAGE_FLOW_SNAPSHOTS_HISTORICAL_KEY_BYTES: ColumnId = ColumnId(1587);
	pub const STORAGE_FLOW_SNAPSHOTS_HISTORICAL_VALUE_BYTES: ColumnId = ColumnId(1588);
	pub const STORAGE_FLOW_SNAPSHOTS_HISTORICAL_TOTAL_BYTES: ColumnId = ColumnId(1589);
	pub const STORAGE_FLOW_SNAPSHOTS_HISTORICAL_COUNT: ColumnId = ColumnId(1590);
	pub const STORAGE_FLOW_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1591);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_TS: ColumnId = ColumnId(1592);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_ID: ColumnId = ColumnId(1593);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1594);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_TIER: ColumnId = ColumnId(1595);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_CURRENT_KEY_BYTES: ColumnId = ColumnId(1596);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_CURRENT_VALUE_BYTES: ColumnId = ColumnId(1597);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_CURRENT_TOTAL_BYTES: ColumnId = ColumnId(1598);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_CURRENT_COUNT: ColumnId = ColumnId(1599);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_HISTORICAL_KEY_BYTES: ColumnId = ColumnId(1600);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_HISTORICAL_VALUE_BYTES: ColumnId = ColumnId(1601);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_HISTORICAL_TOTAL_BYTES: ColumnId = ColumnId(1602);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_HISTORICAL_COUNT: ColumnId = ColumnId(1603);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1604);
	pub const STORAGE_SYSTEM_SNAPSHOTS_TS: ColumnId = ColumnId(1605);
	pub const STORAGE_SYSTEM_SNAPSHOTS_ID: ColumnId = ColumnId(1606);
	pub const STORAGE_SYSTEM_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1607);
	pub const STORAGE_SYSTEM_SNAPSHOTS_TIER: ColumnId = ColumnId(1608);
	pub const STORAGE_SYSTEM_SNAPSHOTS_CURRENT_KEY_BYTES: ColumnId = ColumnId(1609);
	pub const STORAGE_SYSTEM_SNAPSHOTS_CURRENT_VALUE_BYTES: ColumnId = ColumnId(1610);
	pub const STORAGE_SYSTEM_SNAPSHOTS_CURRENT_TOTAL_BYTES: ColumnId = ColumnId(1611);
	pub const STORAGE_SYSTEM_SNAPSHOTS_CURRENT_COUNT: ColumnId = ColumnId(1612);
	pub const STORAGE_SYSTEM_SNAPSHOTS_HISTORICAL_KEY_BYTES: ColumnId = ColumnId(1613);
	pub const STORAGE_SYSTEM_SNAPSHOTS_HISTORICAL_VALUE_BYTES: ColumnId = ColumnId(1614);
	pub const STORAGE_SYSTEM_SNAPSHOTS_HISTORICAL_TOTAL_BYTES: ColumnId = ColumnId(1615);
	pub const STORAGE_SYSTEM_SNAPSHOTS_HISTORICAL_COUNT: ColumnId = ColumnId(1616);
	pub const STORAGE_SYSTEM_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1617);
	pub const CDC_TABLE_SNAPSHOTS_TS: ColumnId = ColumnId(1618);
	pub const CDC_TABLE_SNAPSHOTS_ID: ColumnId = ColumnId(1619);
	pub const CDC_TABLE_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1620);
	pub const CDC_TABLE_SNAPSHOTS_KEY_BYTES: ColumnId = ColumnId(1621);
	pub const CDC_TABLE_SNAPSHOTS_VALUE_BYTES: ColumnId = ColumnId(1622);
	pub const CDC_TABLE_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1623);
	pub const CDC_TABLE_SNAPSHOTS_COUNT: ColumnId = ColumnId(1624);
	pub const CDC_VIEW_SNAPSHOTS_TS: ColumnId = ColumnId(1625);
	pub const CDC_VIEW_SNAPSHOTS_ID: ColumnId = ColumnId(1626);
	pub const CDC_VIEW_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1627);
	pub const CDC_VIEW_SNAPSHOTS_KEY_BYTES: ColumnId = ColumnId(1628);
	pub const CDC_VIEW_SNAPSHOTS_VALUE_BYTES: ColumnId = ColumnId(1629);
	pub const CDC_VIEW_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1630);
	pub const CDC_VIEW_SNAPSHOTS_COUNT: ColumnId = ColumnId(1631);
	pub const CDC_TABLE_VIRTUAL_SNAPSHOTS_TS: ColumnId = ColumnId(1632);
	pub const CDC_TABLE_VIRTUAL_SNAPSHOTS_ID: ColumnId = ColumnId(1633);
	pub const CDC_TABLE_VIRTUAL_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1634);
	pub const CDC_TABLE_VIRTUAL_SNAPSHOTS_KEY_BYTES: ColumnId = ColumnId(1635);
	pub const CDC_TABLE_VIRTUAL_SNAPSHOTS_VALUE_BYTES: ColumnId = ColumnId(1636);
	pub const CDC_TABLE_VIRTUAL_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1637);
	pub const CDC_TABLE_VIRTUAL_SNAPSHOTS_COUNT: ColumnId = ColumnId(1638);
	pub const CDC_RINGBUFFER_SNAPSHOTS_TS: ColumnId = ColumnId(1639);
	pub const CDC_RINGBUFFER_SNAPSHOTS_ID: ColumnId = ColumnId(1640);
	pub const CDC_RINGBUFFER_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1641);
	pub const CDC_RINGBUFFER_SNAPSHOTS_KEY_BYTES: ColumnId = ColumnId(1642);
	pub const CDC_RINGBUFFER_SNAPSHOTS_VALUE_BYTES: ColumnId = ColumnId(1643);
	pub const CDC_RINGBUFFER_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1644);
	pub const CDC_RINGBUFFER_SNAPSHOTS_COUNT: ColumnId = ColumnId(1645);
	pub const CDC_DICTIONARY_SNAPSHOTS_TS: ColumnId = ColumnId(1646);
	pub const CDC_DICTIONARY_SNAPSHOTS_ID: ColumnId = ColumnId(1647);
	pub const CDC_DICTIONARY_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1648);
	pub const CDC_DICTIONARY_SNAPSHOTS_KEY_BYTES: ColumnId = ColumnId(1649);
	pub const CDC_DICTIONARY_SNAPSHOTS_VALUE_BYTES: ColumnId = ColumnId(1650);
	pub const CDC_DICTIONARY_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1651);
	pub const CDC_DICTIONARY_SNAPSHOTS_COUNT: ColumnId = ColumnId(1652);
	pub const CDC_SERIES_SNAPSHOTS_TS: ColumnId = ColumnId(1653);
	pub const CDC_SERIES_SNAPSHOTS_ID: ColumnId = ColumnId(1654);
	pub const CDC_SERIES_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1655);
	pub const CDC_SERIES_SNAPSHOTS_KEY_BYTES: ColumnId = ColumnId(1656);
	pub const CDC_SERIES_SNAPSHOTS_VALUE_BYTES: ColumnId = ColumnId(1657);
	pub const CDC_SERIES_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1658);
	pub const CDC_SERIES_SNAPSHOTS_COUNT: ColumnId = ColumnId(1659);
	pub const CDC_FLOW_SNAPSHOTS_TS: ColumnId = ColumnId(1660);
	pub const CDC_FLOW_SNAPSHOTS_ID: ColumnId = ColumnId(1661);
	pub const CDC_FLOW_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1662);
	pub const CDC_FLOW_SNAPSHOTS_KEY_BYTES: ColumnId = ColumnId(1663);
	pub const CDC_FLOW_SNAPSHOTS_VALUE_BYTES: ColumnId = ColumnId(1664);
	pub const CDC_FLOW_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1665);
	pub const CDC_FLOW_SNAPSHOTS_COUNT: ColumnId = ColumnId(1666);
	pub const CDC_FLOW_NODE_SNAPSHOTS_TS: ColumnId = ColumnId(1667);
	pub const CDC_FLOW_NODE_SNAPSHOTS_ID: ColumnId = ColumnId(1668);
	pub const CDC_FLOW_NODE_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1669);
	pub const CDC_FLOW_NODE_SNAPSHOTS_KEY_BYTES: ColumnId = ColumnId(1670);
	pub const CDC_FLOW_NODE_SNAPSHOTS_VALUE_BYTES: ColumnId = ColumnId(1671);
	pub const CDC_FLOW_NODE_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1672);
	pub const CDC_FLOW_NODE_SNAPSHOTS_COUNT: ColumnId = ColumnId(1673);
	pub const CDC_SYSTEM_SNAPSHOTS_TS: ColumnId = ColumnId(1674);
	pub const CDC_SYSTEM_SNAPSHOTS_ID: ColumnId = ColumnId(1675);
	pub const CDC_SYSTEM_SNAPSHOTS_NAMESPACE_ID: ColumnId = ColumnId(1676);
	pub const CDC_SYSTEM_SNAPSHOTS_KEY_BYTES: ColumnId = ColumnId(1677);
	pub const CDC_SYSTEM_SNAPSHOTS_VALUE_BYTES: ColumnId = ColumnId(1678);
	pub const CDC_SYSTEM_SNAPSHOTS_TOTAL_BYTES: ColumnId = ColumnId(1679);
	pub const CDC_SYSTEM_SNAPSHOTS_COUNT: ColumnId = ColumnId(1680);

	pub const PROFILER_QUERY_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_QUERY_SNAPSHOTS_TS,
		Self::PROFILER_QUERY_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_QUERY_SNAPSHOTS_DIM_1,
		Self::PROFILER_QUERY_SNAPSHOTS_DIM_2,
		Self::PROFILER_QUERY_SNAPSHOTS_CALLS,
		Self::PROFILER_QUERY_SNAPSHOTS_TOTAL,
		Self::PROFILER_QUERY_SNAPSHOTS_MIN,
		Self::PROFILER_QUERY_SNAPSHOTS_MAX,
		Self::PROFILER_QUERY_SNAPSHOTS_P50,
		Self::PROFILER_QUERY_SNAPSHOTS_P60,
		Self::PROFILER_QUERY_SNAPSHOTS_P70,
		Self::PROFILER_QUERY_SNAPSHOTS_P75,
		Self::PROFILER_QUERY_SNAPSHOTS_P80,
		Self::PROFILER_QUERY_SNAPSHOTS_P85,
		Self::PROFILER_QUERY_SNAPSHOTS_P90,
		Self::PROFILER_QUERY_SNAPSHOTS_P95,
		Self::PROFILER_QUERY_SNAPSHOTS_P98,
		Self::PROFILER_QUERY_SNAPSHOTS_P99,
		Self::PROFILER_QUERY_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_QUERY_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_QUERY_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_QUERY_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_TXN_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_TXN_SNAPSHOTS_TS,
		Self::PROFILER_TXN_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_TXN_SNAPSHOTS_DIM_1,
		Self::PROFILER_TXN_SNAPSHOTS_DIM_2,
		Self::PROFILER_TXN_SNAPSHOTS_CALLS,
		Self::PROFILER_TXN_SNAPSHOTS_TOTAL,
		Self::PROFILER_TXN_SNAPSHOTS_MIN,
		Self::PROFILER_TXN_SNAPSHOTS_MAX,
		Self::PROFILER_TXN_SNAPSHOTS_P50,
		Self::PROFILER_TXN_SNAPSHOTS_P60,
		Self::PROFILER_TXN_SNAPSHOTS_P70,
		Self::PROFILER_TXN_SNAPSHOTS_P75,
		Self::PROFILER_TXN_SNAPSHOTS_P80,
		Self::PROFILER_TXN_SNAPSHOTS_P85,
		Self::PROFILER_TXN_SNAPSHOTS_P90,
		Self::PROFILER_TXN_SNAPSHOTS_P95,
		Self::PROFILER_TXN_SNAPSHOTS_P98,
		Self::PROFILER_TXN_SNAPSHOTS_P99,
		Self::PROFILER_TXN_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_TXN_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_TXN_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_TXN_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_STORAGE_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_STORAGE_SNAPSHOTS_TS,
		Self::PROFILER_STORAGE_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_STORAGE_SNAPSHOTS_DIM_1,
		Self::PROFILER_STORAGE_SNAPSHOTS_DIM_2,
		Self::PROFILER_STORAGE_SNAPSHOTS_CALLS,
		Self::PROFILER_STORAGE_SNAPSHOTS_TOTAL,
		Self::PROFILER_STORAGE_SNAPSHOTS_MIN,
		Self::PROFILER_STORAGE_SNAPSHOTS_MAX,
		Self::PROFILER_STORAGE_SNAPSHOTS_P50,
		Self::PROFILER_STORAGE_SNAPSHOTS_P60,
		Self::PROFILER_STORAGE_SNAPSHOTS_P70,
		Self::PROFILER_STORAGE_SNAPSHOTS_P75,
		Self::PROFILER_STORAGE_SNAPSHOTS_P80,
		Self::PROFILER_STORAGE_SNAPSHOTS_P85,
		Self::PROFILER_STORAGE_SNAPSHOTS_P90,
		Self::PROFILER_STORAGE_SNAPSHOTS_P95,
		Self::PROFILER_STORAGE_SNAPSHOTS_P98,
		Self::PROFILER_STORAGE_SNAPSHOTS_P99,
		Self::PROFILER_STORAGE_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_STORAGE_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_STORAGE_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_STORAGE_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_PLAN_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_PLAN_SNAPSHOTS_TS,
		Self::PROFILER_PLAN_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_PLAN_SNAPSHOTS_DIM_1,
		Self::PROFILER_PLAN_SNAPSHOTS_DIM_2,
		Self::PROFILER_PLAN_SNAPSHOTS_CALLS,
		Self::PROFILER_PLAN_SNAPSHOTS_TOTAL,
		Self::PROFILER_PLAN_SNAPSHOTS_MIN,
		Self::PROFILER_PLAN_SNAPSHOTS_MAX,
		Self::PROFILER_PLAN_SNAPSHOTS_P50,
		Self::PROFILER_PLAN_SNAPSHOTS_P60,
		Self::PROFILER_PLAN_SNAPSHOTS_P70,
		Self::PROFILER_PLAN_SNAPSHOTS_P75,
		Self::PROFILER_PLAN_SNAPSHOTS_P80,
		Self::PROFILER_PLAN_SNAPSHOTS_P85,
		Self::PROFILER_PLAN_SNAPSHOTS_P90,
		Self::PROFILER_PLAN_SNAPSHOTS_P95,
		Self::PROFILER_PLAN_SNAPSHOTS_P98,
		Self::PROFILER_PLAN_SNAPSHOTS_P99,
		Self::PROFILER_PLAN_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_PLAN_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_PLAN_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_PLAN_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_CDC_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_CDC_SNAPSHOTS_TS,
		Self::PROFILER_CDC_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_CDC_SNAPSHOTS_DIM_1,
		Self::PROFILER_CDC_SNAPSHOTS_DIM_2,
		Self::PROFILER_CDC_SNAPSHOTS_CALLS,
		Self::PROFILER_CDC_SNAPSHOTS_TOTAL,
		Self::PROFILER_CDC_SNAPSHOTS_MIN,
		Self::PROFILER_CDC_SNAPSHOTS_MAX,
		Self::PROFILER_CDC_SNAPSHOTS_P50,
		Self::PROFILER_CDC_SNAPSHOTS_P60,
		Self::PROFILER_CDC_SNAPSHOTS_P70,
		Self::PROFILER_CDC_SNAPSHOTS_P75,
		Self::PROFILER_CDC_SNAPSHOTS_P80,
		Self::PROFILER_CDC_SNAPSHOTS_P85,
		Self::PROFILER_CDC_SNAPSHOTS_P90,
		Self::PROFILER_CDC_SNAPSHOTS_P95,
		Self::PROFILER_CDC_SNAPSHOTS_P98,
		Self::PROFILER_CDC_SNAPSHOTS_P99,
		Self::PROFILER_CDC_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_CDC_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_CDC_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_CDC_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_FLOW_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_FLOW_SNAPSHOTS_TS,
		Self::PROFILER_FLOW_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_FLOW_SNAPSHOTS_DIM_1,
		Self::PROFILER_FLOW_SNAPSHOTS_DIM_2,
		Self::PROFILER_FLOW_SNAPSHOTS_CALLS,
		Self::PROFILER_FLOW_SNAPSHOTS_TOTAL,
		Self::PROFILER_FLOW_SNAPSHOTS_MIN,
		Self::PROFILER_FLOW_SNAPSHOTS_MAX,
		Self::PROFILER_FLOW_SNAPSHOTS_P50,
		Self::PROFILER_FLOW_SNAPSHOTS_P60,
		Self::PROFILER_FLOW_SNAPSHOTS_P70,
		Self::PROFILER_FLOW_SNAPSHOTS_P75,
		Self::PROFILER_FLOW_SNAPSHOTS_P80,
		Self::PROFILER_FLOW_SNAPSHOTS_P85,
		Self::PROFILER_FLOW_SNAPSHOTS_P90,
		Self::PROFILER_FLOW_SNAPSHOTS_P95,
		Self::PROFILER_FLOW_SNAPSHOTS_P98,
		Self::PROFILER_FLOW_SNAPSHOTS_P99,
		Self::PROFILER_FLOW_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_FLOW_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_FLOW_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_FLOW_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_TS,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_DIM_1,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_DIM_2,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_CALLS,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_TOTAL,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_MIN,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_MAX,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_P50,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_P60,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_P70,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_P75,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_P80,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_P85,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_P90,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_P95,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_P98,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_P99,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_SUBSCRIPTION_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_SERVER_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_SERVER_SNAPSHOTS_TS,
		Self::PROFILER_SERVER_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_SERVER_SNAPSHOTS_DIM_1,
		Self::PROFILER_SERVER_SNAPSHOTS_DIM_2,
		Self::PROFILER_SERVER_SNAPSHOTS_CALLS,
		Self::PROFILER_SERVER_SNAPSHOTS_TOTAL,
		Self::PROFILER_SERVER_SNAPSHOTS_MIN,
		Self::PROFILER_SERVER_SNAPSHOTS_MAX,
		Self::PROFILER_SERVER_SNAPSHOTS_P50,
		Self::PROFILER_SERVER_SNAPSHOTS_P60,
		Self::PROFILER_SERVER_SNAPSHOTS_P70,
		Self::PROFILER_SERVER_SNAPSHOTS_P75,
		Self::PROFILER_SERVER_SNAPSHOTS_P80,
		Self::PROFILER_SERVER_SNAPSHOTS_P85,
		Self::PROFILER_SERVER_SNAPSHOTS_P90,
		Self::PROFILER_SERVER_SNAPSHOTS_P95,
		Self::PROFILER_SERVER_SNAPSHOTS_P98,
		Self::PROFILER_SERVER_SNAPSHOTS_P99,
		Self::PROFILER_SERVER_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_SERVER_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_SERVER_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_SERVER_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_WIRE_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_WIRE_SNAPSHOTS_TS,
		Self::PROFILER_WIRE_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_WIRE_SNAPSHOTS_DIM_1,
		Self::PROFILER_WIRE_SNAPSHOTS_DIM_2,
		Self::PROFILER_WIRE_SNAPSHOTS_CALLS,
		Self::PROFILER_WIRE_SNAPSHOTS_TOTAL,
		Self::PROFILER_WIRE_SNAPSHOTS_MIN,
		Self::PROFILER_WIRE_SNAPSHOTS_MAX,
		Self::PROFILER_WIRE_SNAPSHOTS_P50,
		Self::PROFILER_WIRE_SNAPSHOTS_P60,
		Self::PROFILER_WIRE_SNAPSHOTS_P70,
		Self::PROFILER_WIRE_SNAPSHOTS_P75,
		Self::PROFILER_WIRE_SNAPSHOTS_P80,
		Self::PROFILER_WIRE_SNAPSHOTS_P85,
		Self::PROFILER_WIRE_SNAPSHOTS_P90,
		Self::PROFILER_WIRE_SNAPSHOTS_P95,
		Self::PROFILER_WIRE_SNAPSHOTS_P98,
		Self::PROFILER_WIRE_SNAPSHOTS_P99,
		Self::PROFILER_WIRE_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_WIRE_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_WIRE_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_WIRE_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_AUTH_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_AUTH_SNAPSHOTS_TS,
		Self::PROFILER_AUTH_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_AUTH_SNAPSHOTS_DIM_1,
		Self::PROFILER_AUTH_SNAPSHOTS_DIM_2,
		Self::PROFILER_AUTH_SNAPSHOTS_CALLS,
		Self::PROFILER_AUTH_SNAPSHOTS_TOTAL,
		Self::PROFILER_AUTH_SNAPSHOTS_MIN,
		Self::PROFILER_AUTH_SNAPSHOTS_MAX,
		Self::PROFILER_AUTH_SNAPSHOTS_P50,
		Self::PROFILER_AUTH_SNAPSHOTS_P60,
		Self::PROFILER_AUTH_SNAPSHOTS_P70,
		Self::PROFILER_AUTH_SNAPSHOTS_P75,
		Self::PROFILER_AUTH_SNAPSHOTS_P80,
		Self::PROFILER_AUTH_SNAPSHOTS_P85,
		Self::PROFILER_AUTH_SNAPSHOTS_P90,
		Self::PROFILER_AUTH_SNAPSHOTS_P95,
		Self::PROFILER_AUTH_SNAPSHOTS_P98,
		Self::PROFILER_AUTH_SNAPSHOTS_P99,
		Self::PROFILER_AUTH_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_AUTH_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_AUTH_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_AUTH_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_CATALOG_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_CATALOG_SNAPSHOTS_TS,
		Self::PROFILER_CATALOG_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_CATALOG_SNAPSHOTS_DIM_1,
		Self::PROFILER_CATALOG_SNAPSHOTS_DIM_2,
		Self::PROFILER_CATALOG_SNAPSHOTS_CALLS,
		Self::PROFILER_CATALOG_SNAPSHOTS_TOTAL,
		Self::PROFILER_CATALOG_SNAPSHOTS_MIN,
		Self::PROFILER_CATALOG_SNAPSHOTS_MAX,
		Self::PROFILER_CATALOG_SNAPSHOTS_P50,
		Self::PROFILER_CATALOG_SNAPSHOTS_P60,
		Self::PROFILER_CATALOG_SNAPSHOTS_P70,
		Self::PROFILER_CATALOG_SNAPSHOTS_P75,
		Self::PROFILER_CATALOG_SNAPSHOTS_P80,
		Self::PROFILER_CATALOG_SNAPSHOTS_P85,
		Self::PROFILER_CATALOG_SNAPSHOTS_P90,
		Self::PROFILER_CATALOG_SNAPSHOTS_P95,
		Self::PROFILER_CATALOG_SNAPSHOTS_P98,
		Self::PROFILER_CATALOG_SNAPSHOTS_P99,
		Self::PROFILER_CATALOG_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_CATALOG_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_CATALOG_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_CATALOG_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_ENGINE_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_ENGINE_SNAPSHOTS_TS,
		Self::PROFILER_ENGINE_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_ENGINE_SNAPSHOTS_DIM_1,
		Self::PROFILER_ENGINE_SNAPSHOTS_DIM_2,
		Self::PROFILER_ENGINE_SNAPSHOTS_CALLS,
		Self::PROFILER_ENGINE_SNAPSHOTS_TOTAL,
		Self::PROFILER_ENGINE_SNAPSHOTS_MIN,
		Self::PROFILER_ENGINE_SNAPSHOTS_MAX,
		Self::PROFILER_ENGINE_SNAPSHOTS_P50,
		Self::PROFILER_ENGINE_SNAPSHOTS_P60,
		Self::PROFILER_ENGINE_SNAPSHOTS_P70,
		Self::PROFILER_ENGINE_SNAPSHOTS_P75,
		Self::PROFILER_ENGINE_SNAPSHOTS_P80,
		Self::PROFILER_ENGINE_SNAPSHOTS_P85,
		Self::PROFILER_ENGINE_SNAPSHOTS_P90,
		Self::PROFILER_ENGINE_SNAPSHOTS_P95,
		Self::PROFILER_ENGINE_SNAPSHOTS_P98,
		Self::PROFILER_ENGINE_SNAPSHOTS_P99,
		Self::PROFILER_ENGINE_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_ENGINE_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_ENGINE_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_ENGINE_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_MUTATE_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_MUTATE_SNAPSHOTS_TS,
		Self::PROFILER_MUTATE_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_MUTATE_SNAPSHOTS_DIM_1,
		Self::PROFILER_MUTATE_SNAPSHOTS_DIM_2,
		Self::PROFILER_MUTATE_SNAPSHOTS_CALLS,
		Self::PROFILER_MUTATE_SNAPSHOTS_TOTAL,
		Self::PROFILER_MUTATE_SNAPSHOTS_MIN,
		Self::PROFILER_MUTATE_SNAPSHOTS_MAX,
		Self::PROFILER_MUTATE_SNAPSHOTS_P50,
		Self::PROFILER_MUTATE_SNAPSHOTS_P60,
		Self::PROFILER_MUTATE_SNAPSHOTS_P70,
		Self::PROFILER_MUTATE_SNAPSHOTS_P75,
		Self::PROFILER_MUTATE_SNAPSHOTS_P80,
		Self::PROFILER_MUTATE_SNAPSHOTS_P85,
		Self::PROFILER_MUTATE_SNAPSHOTS_P90,
		Self::PROFILER_MUTATE_SNAPSHOTS_P95,
		Self::PROFILER_MUTATE_SNAPSHOTS_P98,
		Self::PROFILER_MUTATE_SNAPSHOTS_P99,
		Self::PROFILER_MUTATE_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_MUTATE_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_MUTATE_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_MUTATE_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_TRANSPORT_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_TRANSPORT_SNAPSHOTS_TS,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_DIM_1,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_DIM_2,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_CALLS,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_TOTAL,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_MIN,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_MAX,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_P50,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_P60,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_P70,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_P75,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_P80,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_P85,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_P90,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_P95,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_P98,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_P99,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_TRANSPORT_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_TASK_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_TASK_SNAPSHOTS_TS,
		Self::PROFILER_TASK_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_TASK_SNAPSHOTS_DIM_1,
		Self::PROFILER_TASK_SNAPSHOTS_DIM_2,
		Self::PROFILER_TASK_SNAPSHOTS_CALLS,
		Self::PROFILER_TASK_SNAPSHOTS_TOTAL,
		Self::PROFILER_TASK_SNAPSHOTS_MIN,
		Self::PROFILER_TASK_SNAPSHOTS_MAX,
		Self::PROFILER_TASK_SNAPSHOTS_P50,
		Self::PROFILER_TASK_SNAPSHOTS_P60,
		Self::PROFILER_TASK_SNAPSHOTS_P70,
		Self::PROFILER_TASK_SNAPSHOTS_P75,
		Self::PROFILER_TASK_SNAPSHOTS_P80,
		Self::PROFILER_TASK_SNAPSHOTS_P85,
		Self::PROFILER_TASK_SNAPSHOTS_P90,
		Self::PROFILER_TASK_SNAPSHOTS_P95,
		Self::PROFILER_TASK_SNAPSHOTS_P98,
		Self::PROFILER_TASK_SNAPSHOTS_P99,
		Self::PROFILER_TASK_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_TASK_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_TASK_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_TASK_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_POLICY_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_POLICY_SNAPSHOTS_TS,
		Self::PROFILER_POLICY_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_POLICY_SNAPSHOTS_DIM_1,
		Self::PROFILER_POLICY_SNAPSHOTS_DIM_2,
		Self::PROFILER_POLICY_SNAPSHOTS_CALLS,
		Self::PROFILER_POLICY_SNAPSHOTS_TOTAL,
		Self::PROFILER_POLICY_SNAPSHOTS_MIN,
		Self::PROFILER_POLICY_SNAPSHOTS_MAX,
		Self::PROFILER_POLICY_SNAPSHOTS_P50,
		Self::PROFILER_POLICY_SNAPSHOTS_P60,
		Self::PROFILER_POLICY_SNAPSHOTS_P70,
		Self::PROFILER_POLICY_SNAPSHOTS_P75,
		Self::PROFILER_POLICY_SNAPSHOTS_P80,
		Self::PROFILER_POLICY_SNAPSHOTS_P85,
		Self::PROFILER_POLICY_SNAPSHOTS_P90,
		Self::PROFILER_POLICY_SNAPSHOTS_P95,
		Self::PROFILER_POLICY_SNAPSHOTS_P98,
		Self::PROFILER_POLICY_SNAPSHOTS_P99,
		Self::PROFILER_POLICY_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_POLICY_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_POLICY_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_POLICY_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_FFI_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_FFI_SNAPSHOTS_TS,
		Self::PROFILER_FFI_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_FFI_SNAPSHOTS_DIM_1,
		Self::PROFILER_FFI_SNAPSHOTS_DIM_2,
		Self::PROFILER_FFI_SNAPSHOTS_CALLS,
		Self::PROFILER_FFI_SNAPSHOTS_TOTAL,
		Self::PROFILER_FFI_SNAPSHOTS_MIN,
		Self::PROFILER_FFI_SNAPSHOTS_MAX,
		Self::PROFILER_FFI_SNAPSHOTS_P50,
		Self::PROFILER_FFI_SNAPSHOTS_P60,
		Self::PROFILER_FFI_SNAPSHOTS_P70,
		Self::PROFILER_FFI_SNAPSHOTS_P75,
		Self::PROFILER_FFI_SNAPSHOTS_P80,
		Self::PROFILER_FFI_SNAPSHOTS_P85,
		Self::PROFILER_FFI_SNAPSHOTS_P90,
		Self::PROFILER_FFI_SNAPSHOTS_P95,
		Self::PROFILER_FFI_SNAPSHOTS_P98,
		Self::PROFILER_FFI_SNAPSHOTS_P99,
		Self::PROFILER_FFI_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_FFI_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_FFI_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_FFI_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_CACHE_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_CACHE_SNAPSHOTS_TS,
		Self::PROFILER_CACHE_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_CACHE_SNAPSHOTS_DIM_1,
		Self::PROFILER_CACHE_SNAPSHOTS_DIM_2,
		Self::PROFILER_CACHE_SNAPSHOTS_CALLS,
		Self::PROFILER_CACHE_SNAPSHOTS_TOTAL,
		Self::PROFILER_CACHE_SNAPSHOTS_MIN,
		Self::PROFILER_CACHE_SNAPSHOTS_MAX,
		Self::PROFILER_CACHE_SNAPSHOTS_P50,
		Self::PROFILER_CACHE_SNAPSHOTS_P60,
		Self::PROFILER_CACHE_SNAPSHOTS_P70,
		Self::PROFILER_CACHE_SNAPSHOTS_P75,
		Self::PROFILER_CACHE_SNAPSHOTS_P80,
		Self::PROFILER_CACHE_SNAPSHOTS_P85,
		Self::PROFILER_CACHE_SNAPSHOTS_P90,
		Self::PROFILER_CACHE_SNAPSHOTS_P95,
		Self::PROFILER_CACHE_SNAPSHOTS_P98,
		Self::PROFILER_CACHE_SNAPSHOTS_P99,
		Self::PROFILER_CACHE_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_CACHE_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_CACHE_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_CACHE_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_SHAPE_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_SHAPE_SNAPSHOTS_TS,
		Self::PROFILER_SHAPE_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_SHAPE_SNAPSHOTS_DIM_1,
		Self::PROFILER_SHAPE_SNAPSHOTS_DIM_2,
		Self::PROFILER_SHAPE_SNAPSHOTS_CALLS,
		Self::PROFILER_SHAPE_SNAPSHOTS_TOTAL,
		Self::PROFILER_SHAPE_SNAPSHOTS_MIN,
		Self::PROFILER_SHAPE_SNAPSHOTS_MAX,
		Self::PROFILER_SHAPE_SNAPSHOTS_P50,
		Self::PROFILER_SHAPE_SNAPSHOTS_P60,
		Self::PROFILER_SHAPE_SNAPSHOTS_P70,
		Self::PROFILER_SHAPE_SNAPSHOTS_P75,
		Self::PROFILER_SHAPE_SNAPSHOTS_P80,
		Self::PROFILER_SHAPE_SNAPSHOTS_P85,
		Self::PROFILER_SHAPE_SNAPSHOTS_P90,
		Self::PROFILER_SHAPE_SNAPSHOTS_P95,
		Self::PROFILER_SHAPE_SNAPSHOTS_P98,
		Self::PROFILER_SHAPE_SNAPSHOTS_P99,
		Self::PROFILER_SHAPE_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_SHAPE_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_SHAPE_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_SHAPE_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_API_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_API_SNAPSHOTS_TS,
		Self::PROFILER_API_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_API_SNAPSHOTS_DIM_1,
		Self::PROFILER_API_SNAPSHOTS_DIM_2,
		Self::PROFILER_API_SNAPSHOTS_CALLS,
		Self::PROFILER_API_SNAPSHOTS_TOTAL,
		Self::PROFILER_API_SNAPSHOTS_MIN,
		Self::PROFILER_API_SNAPSHOTS_MAX,
		Self::PROFILER_API_SNAPSHOTS_P50,
		Self::PROFILER_API_SNAPSHOTS_P60,
		Self::PROFILER_API_SNAPSHOTS_P70,
		Self::PROFILER_API_SNAPSHOTS_P75,
		Self::PROFILER_API_SNAPSHOTS_P80,
		Self::PROFILER_API_SNAPSHOTS_P85,
		Self::PROFILER_API_SNAPSHOTS_P90,
		Self::PROFILER_API_SNAPSHOTS_P95,
		Self::PROFILER_API_SNAPSHOTS_P98,
		Self::PROFILER_API_SNAPSHOTS_P99,
		Self::PROFILER_API_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_API_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_API_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_API_SNAPSHOTS_EXTRA_3,
	];
	pub const PROFILER_ACTOR_SNAPSHOTS_COLUMNS: [ColumnId; 22] = [
		Self::PROFILER_ACTOR_SNAPSHOTS_TS,
		Self::PROFILER_ACTOR_SNAPSHOTS_SPAN_NAME,
		Self::PROFILER_ACTOR_SNAPSHOTS_DIM_1,
		Self::PROFILER_ACTOR_SNAPSHOTS_DIM_2,
		Self::PROFILER_ACTOR_SNAPSHOTS_CALLS,
		Self::PROFILER_ACTOR_SNAPSHOTS_TOTAL,
		Self::PROFILER_ACTOR_SNAPSHOTS_MIN,
		Self::PROFILER_ACTOR_SNAPSHOTS_MAX,
		Self::PROFILER_ACTOR_SNAPSHOTS_P50,
		Self::PROFILER_ACTOR_SNAPSHOTS_P60,
		Self::PROFILER_ACTOR_SNAPSHOTS_P70,
		Self::PROFILER_ACTOR_SNAPSHOTS_P75,
		Self::PROFILER_ACTOR_SNAPSHOTS_P80,
		Self::PROFILER_ACTOR_SNAPSHOTS_P85,
		Self::PROFILER_ACTOR_SNAPSHOTS_P90,
		Self::PROFILER_ACTOR_SNAPSHOTS_P95,
		Self::PROFILER_ACTOR_SNAPSHOTS_P98,
		Self::PROFILER_ACTOR_SNAPSHOTS_P99,
		Self::PROFILER_ACTOR_SNAPSHOTS_EXTRA_0,
		Self::PROFILER_ACTOR_SNAPSHOTS_EXTRA_1,
		Self::PROFILER_ACTOR_SNAPSHOTS_EXTRA_2,
		Self::PROFILER_ACTOR_SNAPSHOTS_EXTRA_3,
	];
	pub const RUNTIME_MEMORY_SNAPSHOTS_COLUMNS: [ColumnId; 5] = [
		Self::RUNTIME_MEMORY_SNAPSHOTS_TS,
		Self::RUNTIME_MEMORY_SNAPSHOTS_SCOPE,
		Self::RUNTIME_MEMORY_SNAPSHOTS_METRIC,
		Self::RUNTIME_MEMORY_SNAPSHOTS_VALUE,
		Self::RUNTIME_MEMORY_SNAPSHOTS_UNIT,
	];
	pub const RUNTIME_WATERMARKS_SNAPSHOTS_COLUMNS: [ColumnId; 5] = [
		Self::RUNTIME_WATERMARKS_SNAPSHOTS_TS,
		Self::RUNTIME_WATERMARKS_SNAPSHOTS_SCOPE,
		Self::RUNTIME_WATERMARKS_SNAPSHOTS_METRIC,
		Self::RUNTIME_WATERMARKS_SNAPSHOTS_VALUE,
		Self::RUNTIME_WATERMARKS_SNAPSHOTS_UNIT,
	];
	pub const RUNTIME_OPERATORS_SNAPSHOTS_COLUMNS: [ColumnId; 5] = [
		Self::RUNTIME_OPERATORS_SNAPSHOTS_TS,
		Self::RUNTIME_OPERATORS_SNAPSHOTS_SCOPE,
		Self::RUNTIME_OPERATORS_SNAPSHOTS_METRIC,
		Self::RUNTIME_OPERATORS_SNAPSHOTS_VALUE,
		Self::RUNTIME_OPERATORS_SNAPSHOTS_UNIT,
	];
	pub const STORAGE_TABLE_SNAPSHOTS_COLUMNS: [ColumnId; 13] = [
		Self::STORAGE_TABLE_SNAPSHOTS_TS,
		Self::STORAGE_TABLE_SNAPSHOTS_ID,
		Self::STORAGE_TABLE_SNAPSHOTS_NAMESPACE_ID,
		Self::STORAGE_TABLE_SNAPSHOTS_TIER,
		Self::STORAGE_TABLE_SNAPSHOTS_CURRENT_KEY_BYTES,
		Self::STORAGE_TABLE_SNAPSHOTS_CURRENT_VALUE_BYTES,
		Self::STORAGE_TABLE_SNAPSHOTS_CURRENT_TOTAL_BYTES,
		Self::STORAGE_TABLE_SNAPSHOTS_CURRENT_COUNT,
		Self::STORAGE_TABLE_SNAPSHOTS_HISTORICAL_KEY_BYTES,
		Self::STORAGE_TABLE_SNAPSHOTS_HISTORICAL_VALUE_BYTES,
		Self::STORAGE_TABLE_SNAPSHOTS_HISTORICAL_TOTAL_BYTES,
		Self::STORAGE_TABLE_SNAPSHOTS_HISTORICAL_COUNT,
		Self::STORAGE_TABLE_SNAPSHOTS_TOTAL_BYTES,
	];

	pub const STORAGE_VIEW_SNAPSHOTS_COLUMNS: [ColumnId; 13] = [
		Self::STORAGE_VIEW_SNAPSHOTS_TS,
		Self::STORAGE_VIEW_SNAPSHOTS_ID,
		Self::STORAGE_VIEW_SNAPSHOTS_NAMESPACE_ID,
		Self::STORAGE_VIEW_SNAPSHOTS_TIER,
		Self::STORAGE_VIEW_SNAPSHOTS_CURRENT_KEY_BYTES,
		Self::STORAGE_VIEW_SNAPSHOTS_CURRENT_VALUE_BYTES,
		Self::STORAGE_VIEW_SNAPSHOTS_CURRENT_TOTAL_BYTES,
		Self::STORAGE_VIEW_SNAPSHOTS_CURRENT_COUNT,
		Self::STORAGE_VIEW_SNAPSHOTS_HISTORICAL_KEY_BYTES,
		Self::STORAGE_VIEW_SNAPSHOTS_HISTORICAL_VALUE_BYTES,
		Self::STORAGE_VIEW_SNAPSHOTS_HISTORICAL_TOTAL_BYTES,
		Self::STORAGE_VIEW_SNAPSHOTS_HISTORICAL_COUNT,
		Self::STORAGE_VIEW_SNAPSHOTS_TOTAL_BYTES,
	];

	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS_COLUMNS: [ColumnId; 13] = [
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_TS,
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_ID,
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_NAMESPACE_ID,
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_TIER,
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_CURRENT_KEY_BYTES,
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_CURRENT_VALUE_BYTES,
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_CURRENT_TOTAL_BYTES,
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_CURRENT_COUNT,
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_HISTORICAL_KEY_BYTES,
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_HISTORICAL_VALUE_BYTES,
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_HISTORICAL_TOTAL_BYTES,
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_HISTORICAL_COUNT,
		Self::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_TOTAL_BYTES,
	];

	pub const STORAGE_RINGBUFFER_SNAPSHOTS_COLUMNS: [ColumnId; 13] = [
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_TS,
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_ID,
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_NAMESPACE_ID,
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_TIER,
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_CURRENT_KEY_BYTES,
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_CURRENT_VALUE_BYTES,
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_CURRENT_TOTAL_BYTES,
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_CURRENT_COUNT,
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_HISTORICAL_KEY_BYTES,
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_HISTORICAL_VALUE_BYTES,
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_HISTORICAL_TOTAL_BYTES,
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_HISTORICAL_COUNT,
		Self::STORAGE_RINGBUFFER_SNAPSHOTS_TOTAL_BYTES,
	];

	pub const STORAGE_DICTIONARY_SNAPSHOTS_COLUMNS: [ColumnId; 13] = [
		Self::STORAGE_DICTIONARY_SNAPSHOTS_TS,
		Self::STORAGE_DICTIONARY_SNAPSHOTS_ID,
		Self::STORAGE_DICTIONARY_SNAPSHOTS_NAMESPACE_ID,
		Self::STORAGE_DICTIONARY_SNAPSHOTS_TIER,
		Self::STORAGE_DICTIONARY_SNAPSHOTS_CURRENT_KEY_BYTES,
		Self::STORAGE_DICTIONARY_SNAPSHOTS_CURRENT_VALUE_BYTES,
		Self::STORAGE_DICTIONARY_SNAPSHOTS_CURRENT_TOTAL_BYTES,
		Self::STORAGE_DICTIONARY_SNAPSHOTS_CURRENT_COUNT,
		Self::STORAGE_DICTIONARY_SNAPSHOTS_HISTORICAL_KEY_BYTES,
		Self::STORAGE_DICTIONARY_SNAPSHOTS_HISTORICAL_VALUE_BYTES,
		Self::STORAGE_DICTIONARY_SNAPSHOTS_HISTORICAL_TOTAL_BYTES,
		Self::STORAGE_DICTIONARY_SNAPSHOTS_HISTORICAL_COUNT,
		Self::STORAGE_DICTIONARY_SNAPSHOTS_TOTAL_BYTES,
	];

	pub const STORAGE_SERIES_SNAPSHOTS_COLUMNS: [ColumnId; 13] = [
		Self::STORAGE_SERIES_SNAPSHOTS_TS,
		Self::STORAGE_SERIES_SNAPSHOTS_ID,
		Self::STORAGE_SERIES_SNAPSHOTS_NAMESPACE_ID,
		Self::STORAGE_SERIES_SNAPSHOTS_TIER,
		Self::STORAGE_SERIES_SNAPSHOTS_CURRENT_KEY_BYTES,
		Self::STORAGE_SERIES_SNAPSHOTS_CURRENT_VALUE_BYTES,
		Self::STORAGE_SERIES_SNAPSHOTS_CURRENT_TOTAL_BYTES,
		Self::STORAGE_SERIES_SNAPSHOTS_CURRENT_COUNT,
		Self::STORAGE_SERIES_SNAPSHOTS_HISTORICAL_KEY_BYTES,
		Self::STORAGE_SERIES_SNAPSHOTS_HISTORICAL_VALUE_BYTES,
		Self::STORAGE_SERIES_SNAPSHOTS_HISTORICAL_TOTAL_BYTES,
		Self::STORAGE_SERIES_SNAPSHOTS_HISTORICAL_COUNT,
		Self::STORAGE_SERIES_SNAPSHOTS_TOTAL_BYTES,
	];

	pub const STORAGE_FLOW_SNAPSHOTS_COLUMNS: [ColumnId; 13] = [
		Self::STORAGE_FLOW_SNAPSHOTS_TS,
		Self::STORAGE_FLOW_SNAPSHOTS_ID,
		Self::STORAGE_FLOW_SNAPSHOTS_NAMESPACE_ID,
		Self::STORAGE_FLOW_SNAPSHOTS_TIER,
		Self::STORAGE_FLOW_SNAPSHOTS_CURRENT_KEY_BYTES,
		Self::STORAGE_FLOW_SNAPSHOTS_CURRENT_VALUE_BYTES,
		Self::STORAGE_FLOW_SNAPSHOTS_CURRENT_TOTAL_BYTES,
		Self::STORAGE_FLOW_SNAPSHOTS_CURRENT_COUNT,
		Self::STORAGE_FLOW_SNAPSHOTS_HISTORICAL_KEY_BYTES,
		Self::STORAGE_FLOW_SNAPSHOTS_HISTORICAL_VALUE_BYTES,
		Self::STORAGE_FLOW_SNAPSHOTS_HISTORICAL_TOTAL_BYTES,
		Self::STORAGE_FLOW_SNAPSHOTS_HISTORICAL_COUNT,
		Self::STORAGE_FLOW_SNAPSHOTS_TOTAL_BYTES,
	];

	pub const STORAGE_FLOW_NODE_SNAPSHOTS_COLUMNS: [ColumnId; 13] = [
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_TS,
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_ID,
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_NAMESPACE_ID,
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_TIER,
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_CURRENT_KEY_BYTES,
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_CURRENT_VALUE_BYTES,
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_CURRENT_TOTAL_BYTES,
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_CURRENT_COUNT,
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_HISTORICAL_KEY_BYTES,
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_HISTORICAL_VALUE_BYTES,
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_HISTORICAL_TOTAL_BYTES,
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_HISTORICAL_COUNT,
		Self::STORAGE_FLOW_NODE_SNAPSHOTS_TOTAL_BYTES,
	];

	pub const STORAGE_SYSTEM_SNAPSHOTS_COLUMNS: [ColumnId; 13] = [
		Self::STORAGE_SYSTEM_SNAPSHOTS_TS,
		Self::STORAGE_SYSTEM_SNAPSHOTS_ID,
		Self::STORAGE_SYSTEM_SNAPSHOTS_NAMESPACE_ID,
		Self::STORAGE_SYSTEM_SNAPSHOTS_TIER,
		Self::STORAGE_SYSTEM_SNAPSHOTS_CURRENT_KEY_BYTES,
		Self::STORAGE_SYSTEM_SNAPSHOTS_CURRENT_VALUE_BYTES,
		Self::STORAGE_SYSTEM_SNAPSHOTS_CURRENT_TOTAL_BYTES,
		Self::STORAGE_SYSTEM_SNAPSHOTS_CURRENT_COUNT,
		Self::STORAGE_SYSTEM_SNAPSHOTS_HISTORICAL_KEY_BYTES,
		Self::STORAGE_SYSTEM_SNAPSHOTS_HISTORICAL_VALUE_BYTES,
		Self::STORAGE_SYSTEM_SNAPSHOTS_HISTORICAL_TOTAL_BYTES,
		Self::STORAGE_SYSTEM_SNAPSHOTS_HISTORICAL_COUNT,
		Self::STORAGE_SYSTEM_SNAPSHOTS_TOTAL_BYTES,
	];

	pub const CDC_TABLE_SNAPSHOTS_COLUMNS: [ColumnId; 7] = [
		Self::CDC_TABLE_SNAPSHOTS_TS,
		Self::CDC_TABLE_SNAPSHOTS_ID,
		Self::CDC_TABLE_SNAPSHOTS_NAMESPACE_ID,
		Self::CDC_TABLE_SNAPSHOTS_KEY_BYTES,
		Self::CDC_TABLE_SNAPSHOTS_VALUE_BYTES,
		Self::CDC_TABLE_SNAPSHOTS_TOTAL_BYTES,
		Self::CDC_TABLE_SNAPSHOTS_COUNT,
	];

	pub const CDC_VIEW_SNAPSHOTS_COLUMNS: [ColumnId; 7] = [
		Self::CDC_VIEW_SNAPSHOTS_TS,
		Self::CDC_VIEW_SNAPSHOTS_ID,
		Self::CDC_VIEW_SNAPSHOTS_NAMESPACE_ID,
		Self::CDC_VIEW_SNAPSHOTS_KEY_BYTES,
		Self::CDC_VIEW_SNAPSHOTS_VALUE_BYTES,
		Self::CDC_VIEW_SNAPSHOTS_TOTAL_BYTES,
		Self::CDC_VIEW_SNAPSHOTS_COUNT,
	];

	pub const CDC_TABLE_VIRTUAL_SNAPSHOTS_COLUMNS: [ColumnId; 7] = [
		Self::CDC_TABLE_VIRTUAL_SNAPSHOTS_TS,
		Self::CDC_TABLE_VIRTUAL_SNAPSHOTS_ID,
		Self::CDC_TABLE_VIRTUAL_SNAPSHOTS_NAMESPACE_ID,
		Self::CDC_TABLE_VIRTUAL_SNAPSHOTS_KEY_BYTES,
		Self::CDC_TABLE_VIRTUAL_SNAPSHOTS_VALUE_BYTES,
		Self::CDC_TABLE_VIRTUAL_SNAPSHOTS_TOTAL_BYTES,
		Self::CDC_TABLE_VIRTUAL_SNAPSHOTS_COUNT,
	];

	pub const CDC_RINGBUFFER_SNAPSHOTS_COLUMNS: [ColumnId; 7] = [
		Self::CDC_RINGBUFFER_SNAPSHOTS_TS,
		Self::CDC_RINGBUFFER_SNAPSHOTS_ID,
		Self::CDC_RINGBUFFER_SNAPSHOTS_NAMESPACE_ID,
		Self::CDC_RINGBUFFER_SNAPSHOTS_KEY_BYTES,
		Self::CDC_RINGBUFFER_SNAPSHOTS_VALUE_BYTES,
		Self::CDC_RINGBUFFER_SNAPSHOTS_TOTAL_BYTES,
		Self::CDC_RINGBUFFER_SNAPSHOTS_COUNT,
	];

	pub const CDC_DICTIONARY_SNAPSHOTS_COLUMNS: [ColumnId; 7] = [
		Self::CDC_DICTIONARY_SNAPSHOTS_TS,
		Self::CDC_DICTIONARY_SNAPSHOTS_ID,
		Self::CDC_DICTIONARY_SNAPSHOTS_NAMESPACE_ID,
		Self::CDC_DICTIONARY_SNAPSHOTS_KEY_BYTES,
		Self::CDC_DICTIONARY_SNAPSHOTS_VALUE_BYTES,
		Self::CDC_DICTIONARY_SNAPSHOTS_TOTAL_BYTES,
		Self::CDC_DICTIONARY_SNAPSHOTS_COUNT,
	];

	pub const CDC_SERIES_SNAPSHOTS_COLUMNS: [ColumnId; 7] = [
		Self::CDC_SERIES_SNAPSHOTS_TS,
		Self::CDC_SERIES_SNAPSHOTS_ID,
		Self::CDC_SERIES_SNAPSHOTS_NAMESPACE_ID,
		Self::CDC_SERIES_SNAPSHOTS_KEY_BYTES,
		Self::CDC_SERIES_SNAPSHOTS_VALUE_BYTES,
		Self::CDC_SERIES_SNAPSHOTS_TOTAL_BYTES,
		Self::CDC_SERIES_SNAPSHOTS_COUNT,
	];

	pub const CDC_FLOW_SNAPSHOTS_COLUMNS: [ColumnId; 7] = [
		Self::CDC_FLOW_SNAPSHOTS_TS,
		Self::CDC_FLOW_SNAPSHOTS_ID,
		Self::CDC_FLOW_SNAPSHOTS_NAMESPACE_ID,
		Self::CDC_FLOW_SNAPSHOTS_KEY_BYTES,
		Self::CDC_FLOW_SNAPSHOTS_VALUE_BYTES,
		Self::CDC_FLOW_SNAPSHOTS_TOTAL_BYTES,
		Self::CDC_FLOW_SNAPSHOTS_COUNT,
	];

	pub const CDC_FLOW_NODE_SNAPSHOTS_COLUMNS: [ColumnId; 7] = [
		Self::CDC_FLOW_NODE_SNAPSHOTS_TS,
		Self::CDC_FLOW_NODE_SNAPSHOTS_ID,
		Self::CDC_FLOW_NODE_SNAPSHOTS_NAMESPACE_ID,
		Self::CDC_FLOW_NODE_SNAPSHOTS_KEY_BYTES,
		Self::CDC_FLOW_NODE_SNAPSHOTS_VALUE_BYTES,
		Self::CDC_FLOW_NODE_SNAPSHOTS_TOTAL_BYTES,
		Self::CDC_FLOW_NODE_SNAPSHOTS_COUNT,
	];

	pub const CDC_SYSTEM_SNAPSHOTS_COLUMNS: [ColumnId; 7] = [
		Self::CDC_SYSTEM_SNAPSHOTS_TS,
		Self::CDC_SYSTEM_SNAPSHOTS_ID,
		Self::CDC_SYSTEM_SNAPSHOTS_NAMESPACE_ID,
		Self::CDC_SYSTEM_SNAPSHOTS_KEY_BYTES,
		Self::CDC_SYSTEM_SNAPSHOTS_VALUE_BYTES,
		Self::CDC_SYSTEM_SNAPSHOTS_TOTAL_BYTES,
		Self::CDC_SYSTEM_SNAPSHOTS_COUNT,
	];
}

impl Deref for ColumnId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ColumnId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ColumnId> for u64 {
	fn from(value: ColumnId) -> Self {
		value.0
	}
}

impl Serialize for ColumnId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ColumnId {
	fn deserialize<D>(deserializer: D) -> Result<ColumnId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ColumnId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(ColumnId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub enum IndexId {
	Primary(PrimaryKeyId),
}

impl IndexId {
	pub fn as_u64(&self) -> u64 {
		match self {
			IndexId::Primary(id) => id.0,
		}
	}

	pub fn primary(id: impl Into<PrimaryKeyId>) -> Self {
		IndexId::Primary(id.into())
	}

	pub fn next(&self) -> IndexId {
		match self {
			IndexId::Primary(primary) => IndexId::Primary(PrimaryKeyId(primary.0 + 1)),
		}
	}

	pub fn prev(&self) -> IndexId {
		match self {
			IndexId::Primary(primary) => IndexId::Primary(PrimaryKeyId(primary.0.wrapping_sub(1))),
		}
	}
}

impl Deref for IndexId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		match self {
			IndexId::Primary(id) => &id.0,
		}
	}
}

impl PartialEq<u64> for IndexId {
	fn eq(&self, other: &u64) -> bool {
		self.as_u64().eq(other)
	}
}

impl From<IndexId> for u64 {
	fn from(value: IndexId) -> Self {
		value.as_u64()
	}
}

impl Serialize for IndexId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.as_u64())
	}
}

impl<'de> Deserialize<'de> for IndexId {
	fn deserialize<D>(deserializer: D) -> Result<IndexId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = IndexId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(IndexId::Primary(PrimaryKeyId(value)))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ColumnPropertyId(pub u64);

impl Deref for ColumnPropertyId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ColumnPropertyId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ColumnPropertyId> for u64 {
	fn from(value: ColumnPropertyId) -> Self {
		value.0
	}
}

impl Serialize for ColumnPropertyId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ColumnPropertyId {
	fn deserialize<D>(deserializer: D) -> Result<ColumnPropertyId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ColumnPropertyId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(ColumnPropertyId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct NamespaceId(pub u64);

impl Display for NamespaceId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for NamespaceId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for NamespaceId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<NamespaceId> for u64 {
	fn from(value: NamespaceId) -> Self {
		value.0
	}
}

impl Serialize for NamespaceId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for NamespaceId {
	fn deserialize<D>(deserializer: D) -> Result<NamespaceId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = NamespaceId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(NamespaceId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct TableId(pub u64);

impl Display for TableId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for TableId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for TableId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<TableId> for u64 {
	fn from(value: TableId) -> Self {
		value.0
	}
}

impl TableId {
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

impl From<i32> for TableId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for TableId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for TableId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for TableId {
	fn deserialize<D>(deserializer: D) -> Result<TableId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = TableId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(TableId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ViewId(pub u64);

impl Display for ViewId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for ViewId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ViewId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ViewId> for u64 {
	fn from(value: ViewId) -> Self {
		value.0
	}
}

impl ViewId {
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

impl From<i32> for ViewId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for ViewId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for ViewId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ViewId {
	fn deserialize<D>(deserializer: D) -> Result<ViewId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ViewId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(ViewId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct PrimaryKeyId(pub u64);

impl Display for PrimaryKeyId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for PrimaryKeyId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for PrimaryKeyId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<PrimaryKeyId> for u64 {
	fn from(value: PrimaryKeyId) -> Self {
		value.0
	}
}

impl From<i32> for PrimaryKeyId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for PrimaryKeyId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for PrimaryKeyId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for PrimaryKeyId {
	fn deserialize<D>(deserializer: D) -> Result<PrimaryKeyId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = PrimaryKeyId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(PrimaryKeyId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct RingBufferId(pub u64);

impl RingBufferId {
	pub const REQUEST_HISTORY: RingBufferId = RingBufferId(1);
	pub const STATEMENT_STATS: RingBufferId = RingBufferId(2);
}

impl Display for RingBufferId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for RingBufferId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for RingBufferId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<RingBufferId> for u64 {
	fn from(value: RingBufferId) -> Self {
		value.0
	}
}

impl RingBufferId {
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

impl From<i32> for RingBufferId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for RingBufferId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for RingBufferId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for RingBufferId {
	fn deserialize<D>(deserializer: D) -> Result<RingBufferId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = RingBufferId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(RingBufferId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ProcedureId(u64);

impl ProcedureId {
	pub const SYSTEM_RESERVED_START: u64 = 1 << 48;

	pub const SYSTEM_CONFIG_SET: ProcedureId = ProcedureId::persistent(1);

	pub const fn persistent(id: u64) -> Self {
		assert!(id < Self::SYSTEM_RESERVED_START, "persistent ProcedureId must be below SYSTEM_RESERVED_START");
		Self(id)
	}

	pub const fn ephemeral(id: u64) -> Self {
		assert!(
			id >= Self::SYSTEM_RESERVED_START,
			"ephemeral ProcedureId must be at or above SYSTEM_RESERVED_START"
		);
		Self(id)
	}

	pub const fn from_raw(id: u64) -> Self {
		Self(id)
	}

	pub const fn is_ephemeral(&self) -> bool {
		self.0 >= Self::SYSTEM_RESERVED_START
	}
}

impl Display for ProcedureId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for ProcedureId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ProcedureId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ProcedureId> for u64 {
	fn from(value: ProcedureId) -> Self {
		value.0
	}
}

impl Serialize for ProcedureId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ProcedureId {
	fn deserialize<D>(deserializer: D) -> Result<ProcedureId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ProcedureId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(ProcedureId::from_raw(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct TestId(pub u64);

impl Display for TestId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for TestId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for TestId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<TestId> for u64 {
	fn from(value: TestId) -> Self {
		value.0
	}
}

impl From<i32> for TestId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for TestId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for TestId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for TestId {
	fn deserialize<D>(deserializer: D) -> Result<TestId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = TestId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(TestId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SubscriptionId(pub u64);

impl Display for SubscriptionId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for SubscriptionId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SubscriptionId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<SubscriptionId> for u64 {
	fn from(value: SubscriptionId) -> Self {
		value.0
	}
}

impl From<u64> for SubscriptionId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for SubscriptionId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SubscriptionId {
	fn deserialize<D>(deserializer: D) -> Result<SubscriptionId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SubscriptionId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SubscriptionId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SequenceId(pub u64);

impl Deref for SequenceId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SequenceId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl Serialize for SequenceId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SequenceId {
	fn deserialize<D>(deserializer: D) -> Result<SequenceId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SequenceId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SequenceId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SubscriptionColumnId(pub u64);

impl Display for SubscriptionColumnId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for SubscriptionColumnId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SubscriptionColumnId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<SubscriptionColumnId> for u64 {
	fn from(value: SubscriptionColumnId) -> Self {
		value.0
	}
}

impl From<i32> for SubscriptionColumnId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for SubscriptionColumnId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for SubscriptionColumnId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SubscriptionColumnId {
	fn deserialize<D>(deserializer: D) -> Result<SubscriptionColumnId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SubscriptionColumnId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SubscriptionColumnId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SeriesId(pub u64);

impl Display for SeriesId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for SeriesId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SeriesId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<SeriesId> for u64 {
	fn from(value: SeriesId) -> Self {
		value.0
	}
}

impl SeriesId {
	pub const PROFILER_QUERY_SNAPSHOTS: SeriesId = SeriesId(1024);
	pub const PROFILER_TXN_SNAPSHOTS: SeriesId = SeriesId(1025);
	pub const PROFILER_STORAGE_SNAPSHOTS: SeriesId = SeriesId(1026);
	pub const PROFILER_PLAN_SNAPSHOTS: SeriesId = SeriesId(1027);
	pub const PROFILER_CDC_SNAPSHOTS: SeriesId = SeriesId(1028);
	pub const PROFILER_FLOW_SNAPSHOTS: SeriesId = SeriesId(1029);
	pub const PROFILER_SUBSCRIPTION_SNAPSHOTS: SeriesId = SeriesId(1030);
	pub const PROFILER_SERVER_SNAPSHOTS: SeriesId = SeriesId(1031);
	pub const PROFILER_WIRE_SNAPSHOTS: SeriesId = SeriesId(1032);
	pub const PROFILER_AUTH_SNAPSHOTS: SeriesId = SeriesId(1033);
	pub const PROFILER_CATALOG_SNAPSHOTS: SeriesId = SeriesId(1034);
	pub const PROFILER_ENGINE_SNAPSHOTS: SeriesId = SeriesId(1035);
	pub const PROFILER_MUTATE_SNAPSHOTS: SeriesId = SeriesId(1036);
	pub const PROFILER_TRANSPORT_SNAPSHOTS: SeriesId = SeriesId(1037);
	pub const PROFILER_TASK_SNAPSHOTS: SeriesId = SeriesId(1038);
	pub const PROFILER_POLICY_SNAPSHOTS: SeriesId = SeriesId(1039);
	pub const PROFILER_FFI_SNAPSHOTS: SeriesId = SeriesId(1040);
	pub const PROFILER_CACHE_SNAPSHOTS: SeriesId = SeriesId(1043);
	pub const PROFILER_SHAPE_SNAPSHOTS: SeriesId = SeriesId(1044);
	pub const PROFILER_API_SNAPSHOTS: SeriesId = SeriesId(1045);
	pub const PROFILER_ACTOR_SNAPSHOTS: SeriesId = SeriesId(1046);
	pub const RUNTIME_MEMORY_SNAPSHOTS: SeriesId = SeriesId(1041);
	pub const RUNTIME_WATERMARKS_SNAPSHOTS: SeriesId = SeriesId(1042);
	pub const RUNTIME_OPERATORS_SNAPSHOTS: SeriesId = SeriesId(1047);
	pub const STORAGE_TABLE_SNAPSHOTS: SeriesId = SeriesId(1048);
	pub const STORAGE_VIEW_SNAPSHOTS: SeriesId = SeriesId(1049);
	pub const STORAGE_TABLE_VIRTUAL_SNAPSHOTS: SeriesId = SeriesId(1050);
	pub const STORAGE_RINGBUFFER_SNAPSHOTS: SeriesId = SeriesId(1051);
	pub const STORAGE_DICTIONARY_SNAPSHOTS: SeriesId = SeriesId(1052);
	pub const STORAGE_SERIES_SNAPSHOTS: SeriesId = SeriesId(1053);
	pub const STORAGE_FLOW_SNAPSHOTS: SeriesId = SeriesId(1054);
	pub const STORAGE_FLOW_NODE_SNAPSHOTS: SeriesId = SeriesId(1055);
	pub const STORAGE_SYSTEM_SNAPSHOTS: SeriesId = SeriesId(1056);
	pub const CDC_TABLE_SNAPSHOTS: SeriesId = SeriesId(1057);
	pub const CDC_VIEW_SNAPSHOTS: SeriesId = SeriesId(1058);
	pub const CDC_TABLE_VIRTUAL_SNAPSHOTS: SeriesId = SeriesId(1059);
	pub const CDC_RINGBUFFER_SNAPSHOTS: SeriesId = SeriesId(1060);
	pub const CDC_DICTIONARY_SNAPSHOTS: SeriesId = SeriesId(1061);
	pub const CDC_SERIES_SNAPSHOTS: SeriesId = SeriesId(1062);
	pub const CDC_FLOW_SNAPSHOTS: SeriesId = SeriesId(1063);
	pub const CDC_FLOW_NODE_SNAPSHOTS: SeriesId = SeriesId(1064);
	pub const CDC_SYSTEM_SNAPSHOTS: SeriesId = SeriesId(1065);

	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

impl From<i32> for SeriesId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for SeriesId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for SeriesId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SeriesId {
	fn deserialize<D>(deserializer: D) -> Result<SeriesId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SeriesId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SeriesId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct HandlerId(pub u64);

impl Display for HandlerId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for HandlerId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for HandlerId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<HandlerId> for u64 {
	fn from(value: HandlerId) -> Self {
		value.0
	}
}

impl From<i32> for HandlerId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for HandlerId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for HandlerId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for HandlerId {
	fn deserialize<D>(deserializer: D) -> Result<HandlerId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = HandlerId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(HandlerId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct MigrationId(pub u64);

impl Display for MigrationId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for MigrationId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for MigrationId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<MigrationId> for u64 {
	fn from(value: MigrationId) -> Self {
		value.0
	}
}

impl From<i32> for MigrationId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for MigrationId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for MigrationId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for MigrationId {
	fn deserialize<D>(deserializer: D) -> Result<MigrationId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = MigrationId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(MigrationId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct MigrationEventId(pub u64);

impl Display for MigrationEventId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for MigrationEventId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for MigrationEventId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<MigrationEventId> for u64 {
	fn from(value: MigrationEventId) -> Self {
		value.0
	}
}

impl From<i32> for MigrationEventId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for MigrationEventId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for MigrationEventId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for MigrationEventId {
	fn deserialize<D>(deserializer: D) -> Result<MigrationEventId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = MigrationEventId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(MigrationEventId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SourceId(pub u64);

impl Display for SourceId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for SourceId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SourceId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<SourceId> for u64 {
	fn from(value: SourceId) -> Self {
		value.0
	}
}

impl From<u64> for SourceId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for SourceId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SourceId {
	fn deserialize<D>(deserializer: D) -> Result<SourceId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SourceId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SourceId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct BindingId(pub u64);

impl Display for BindingId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for BindingId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for BindingId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<BindingId> for u64 {
	fn from(value: BindingId) -> Self {
		value.0
	}
}

impl From<u64> for BindingId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for BindingId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for BindingId {
	fn deserialize<D>(deserializer: D) -> Result<BindingId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = BindingId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(BindingId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ColumnSnapshotId(pub u64);

impl Display for ColumnSnapshotId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for ColumnSnapshotId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ColumnSnapshotId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ColumnSnapshotId> for u64 {
	fn from(value: ColumnSnapshotId) -> Self {
		value.0
	}
}

impl From<u64> for ColumnSnapshotId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for ColumnSnapshotId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ColumnSnapshotId {
	fn deserialize<D>(deserializer: D) -> Result<ColumnSnapshotId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ColumnSnapshotId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(ColumnSnapshotId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SinkId(pub u64);

impl Display for SinkId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for SinkId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SinkId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<SinkId> for u64 {
	fn from(value: SinkId) -> Self {
		value.0
	}
}

impl From<u64> for SinkId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for SinkId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SinkId {
	fn deserialize<D>(deserializer: D) -> Result<SinkId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SinkId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SinkId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

const RESERVED_USER_ID_START: u64 = 16385;

const RESERVED_SOURCE_IDS: [u64; 44] = [
	RingBufferId::REQUEST_HISTORY.0,
	RingBufferId::STATEMENT_STATS.0,
	SeriesId::PROFILER_QUERY_SNAPSHOTS.0,
	SeriesId::PROFILER_TXN_SNAPSHOTS.0,
	SeriesId::PROFILER_STORAGE_SNAPSHOTS.0,
	SeriesId::PROFILER_PLAN_SNAPSHOTS.0,
	SeriesId::PROFILER_CDC_SNAPSHOTS.0,
	SeriesId::PROFILER_FLOW_SNAPSHOTS.0,
	SeriesId::PROFILER_SUBSCRIPTION_SNAPSHOTS.0,
	SeriesId::PROFILER_SERVER_SNAPSHOTS.0,
	SeriesId::PROFILER_WIRE_SNAPSHOTS.0,
	SeriesId::PROFILER_AUTH_SNAPSHOTS.0,
	SeriesId::PROFILER_CATALOG_SNAPSHOTS.0,
	SeriesId::PROFILER_ENGINE_SNAPSHOTS.0,
	SeriesId::PROFILER_MUTATE_SNAPSHOTS.0,
	SeriesId::PROFILER_TRANSPORT_SNAPSHOTS.0,
	SeriesId::PROFILER_TASK_SNAPSHOTS.0,
	SeriesId::PROFILER_POLICY_SNAPSHOTS.0,
	SeriesId::PROFILER_FFI_SNAPSHOTS.0,
	SeriesId::PROFILER_CACHE_SNAPSHOTS.0,
	SeriesId::PROFILER_SHAPE_SNAPSHOTS.0,
	SeriesId::PROFILER_API_SNAPSHOTS.0,
	SeriesId::PROFILER_ACTOR_SNAPSHOTS.0,
	SeriesId::RUNTIME_MEMORY_SNAPSHOTS.0,
	SeriesId::RUNTIME_WATERMARKS_SNAPSHOTS.0,
	SeriesId::RUNTIME_OPERATORS_SNAPSHOTS.0,
	SeriesId::STORAGE_TABLE_SNAPSHOTS.0,
	SeriesId::STORAGE_VIEW_SNAPSHOTS.0,
	SeriesId::STORAGE_TABLE_VIRTUAL_SNAPSHOTS.0,
	SeriesId::STORAGE_RINGBUFFER_SNAPSHOTS.0,
	SeriesId::STORAGE_DICTIONARY_SNAPSHOTS.0,
	SeriesId::STORAGE_SERIES_SNAPSHOTS.0,
	SeriesId::STORAGE_FLOW_SNAPSHOTS.0,
	SeriesId::STORAGE_FLOW_NODE_SNAPSHOTS.0,
	SeriesId::STORAGE_SYSTEM_SNAPSHOTS.0,
	SeriesId::CDC_TABLE_SNAPSHOTS.0,
	SeriesId::CDC_VIEW_SNAPSHOTS.0,
	SeriesId::CDC_TABLE_VIRTUAL_SNAPSHOTS.0,
	SeriesId::CDC_RINGBUFFER_SNAPSHOTS.0,
	SeriesId::CDC_DICTIONARY_SNAPSHOTS.0,
	SeriesId::CDC_SERIES_SNAPSHOTS.0,
	SeriesId::CDC_FLOW_SNAPSHOTS.0,
	SeriesId::CDC_FLOW_NODE_SNAPSHOTS.0,
	SeriesId::CDC_SYSTEM_SNAPSHOTS.0,
];

const RESERVED_RINGBUFFER_COLUMNS: [ColumnId; 18] = [
	ColumnId::REQUEST_HISTORY_TIMESTAMP,
	ColumnId::REQUEST_HISTORY_OPERATION,
	ColumnId::REQUEST_HISTORY_FINGERPRINT,
	ColumnId::REQUEST_HISTORY_TOTAL_DURATION,
	ColumnId::REQUEST_HISTORY_COMPUTE_DURATION,
	ColumnId::REQUEST_HISTORY_SUCCESS,
	ColumnId::REQUEST_HISTORY_STATEMENT_COUNT,
	ColumnId::REQUEST_HISTORY_NORMALIZED_RQL,
	ColumnId::STATEMENT_STATS_SNAPSHOT_TIMESTAMP,
	ColumnId::STATEMENT_STATS_FINGERPRINT,
	ColumnId::STATEMENT_STATS_NORMALIZED_RQL,
	ColumnId::STATEMENT_STATS_CALLS,
	ColumnId::STATEMENT_STATS_TOTAL_DURATION,
	ColumnId::STATEMENT_STATS_MEAN_DURATION,
	ColumnId::STATEMENT_STATS_MAX_DURATION,
	ColumnId::STATEMENT_STATS_MIN_DURATION,
	ColumnId::STATEMENT_STATS_TOTAL_ROWS,
	ColumnId::STATEMENT_STATS_ERRORS,
];

const RESERVED_COLUMN_GROUPS: [&[ColumnId]; 43] = [
	&RESERVED_RINGBUFFER_COLUMNS,
	&ColumnId::PROFILER_QUERY_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_TXN_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_STORAGE_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_PLAN_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_CDC_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_FLOW_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_SUBSCRIPTION_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_SERVER_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_WIRE_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_AUTH_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_CATALOG_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_ENGINE_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_MUTATE_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_TRANSPORT_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_TASK_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_POLICY_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_FFI_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_CACHE_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_SHAPE_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_API_SNAPSHOTS_COLUMNS,
	&ColumnId::PROFILER_ACTOR_SNAPSHOTS_COLUMNS,
	&ColumnId::RUNTIME_MEMORY_SNAPSHOTS_COLUMNS,
	&ColumnId::RUNTIME_WATERMARKS_SNAPSHOTS_COLUMNS,
	&ColumnId::RUNTIME_OPERATORS_SNAPSHOTS_COLUMNS,
	&ColumnId::STORAGE_TABLE_SNAPSHOTS_COLUMNS,
	&ColumnId::STORAGE_VIEW_SNAPSHOTS_COLUMNS,
	&ColumnId::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_COLUMNS,
	&ColumnId::STORAGE_RINGBUFFER_SNAPSHOTS_COLUMNS,
	&ColumnId::STORAGE_DICTIONARY_SNAPSHOTS_COLUMNS,
	&ColumnId::STORAGE_SERIES_SNAPSHOTS_COLUMNS,
	&ColumnId::STORAGE_FLOW_SNAPSHOTS_COLUMNS,
	&ColumnId::STORAGE_FLOW_NODE_SNAPSHOTS_COLUMNS,
	&ColumnId::STORAGE_SYSTEM_SNAPSHOTS_COLUMNS,
	&ColumnId::CDC_TABLE_SNAPSHOTS_COLUMNS,
	&ColumnId::CDC_VIEW_SNAPSHOTS_COLUMNS,
	&ColumnId::CDC_TABLE_VIRTUAL_SNAPSHOTS_COLUMNS,
	&ColumnId::CDC_RINGBUFFER_SNAPSHOTS_COLUMNS,
	&ColumnId::CDC_DICTIONARY_SNAPSHOTS_COLUMNS,
	&ColumnId::CDC_SERIES_SNAPSHOTS_COLUMNS,
	&ColumnId::CDC_FLOW_SNAPSHOTS_COLUMNS,
	&ColumnId::CDC_FLOW_NODE_SNAPSHOTS_COLUMNS,
	&ColumnId::CDC_SYSTEM_SNAPSHOTS_COLUMNS,
];

const fn reserved_u64_all_below(values: &[u64], limit: u64) -> bool {
	let mut i = 0;
	while i < values.len() {
		if values[i] >= limit {
			return false;
		}
		i += 1;
	}
	true
}

const fn reserved_u64_has_duplicate(values: &[u64]) -> bool {
	let mut i = 0;
	while i < values.len() {
		let mut j = i + 1;
		while j < values.len() {
			if values[i] == values[j] {
				return true;
			}
			j += 1;
		}
		i += 1;
	}
	false
}

const fn reserved_columns_all_below(groups: &[&[ColumnId]], limit: u64) -> bool {
	let mut g = 0;
	while g < groups.len() {
		let group = groups[g];
		let mut i = 0;
		while i < group.len() {
			if group[i].0 >= limit {
				return false;
			}
			i += 1;
		}
		g += 1;
	}
	true
}

const fn reserved_columns_has_duplicate(groups: &[&[ColumnId]]) -> bool {
	let mut g1 = 0;
	while g1 < groups.len() {
		let mut i1 = 0;
		while i1 < groups[g1].len() {
			let value = groups[g1][i1].0;
			let mut g2 = g1;
			let mut i2 = i1 + 1;
			while g2 < groups.len() {
				while i2 < groups[g2].len() {
					if groups[g2][i2].0 == value {
						return true;
					}
					i2 += 1;
				}
				g2 += 1;
				i2 = 0;
			}
			i1 += 1;
		}
		g1 += 1;
	}
	false
}

const _: () = {
	assert!(
		reserved_u64_all_below(&RESERVED_SOURCE_IDS, RESERVED_USER_ID_START),
		"reserved system source id leaks into the user range"
	);
	assert!(!reserved_u64_has_duplicate(&RESERVED_SOURCE_IDS), "duplicate reserved system source id");
	assert!(
		reserved_columns_all_below(&RESERVED_COLUMN_GROUPS, RESERVED_USER_ID_START),
		"reserved system column id leaks into the user range"
	);
	assert!(!reserved_columns_has_duplicate(&RESERVED_COLUMN_GROUPS), "duplicate reserved system column id");
};

#[cfg(test)]
mod reserved_id_tests {
	use std::collections::HashSet;

	use super::{ColumnId, RingBufferId, SeriesId};

	const USER_ID_START: u64 = 16385;

	fn reserved_series_ids() -> [SeriesId; 42] {
		[
			SeriesId::PROFILER_QUERY_SNAPSHOTS,
			SeriesId::PROFILER_TXN_SNAPSHOTS,
			SeriesId::PROFILER_STORAGE_SNAPSHOTS,
			SeriesId::PROFILER_PLAN_SNAPSHOTS,
			SeriesId::PROFILER_CDC_SNAPSHOTS,
			SeriesId::PROFILER_FLOW_SNAPSHOTS,
			SeriesId::PROFILER_SUBSCRIPTION_SNAPSHOTS,
			SeriesId::PROFILER_SERVER_SNAPSHOTS,
			SeriesId::PROFILER_WIRE_SNAPSHOTS,
			SeriesId::PROFILER_AUTH_SNAPSHOTS,
			SeriesId::PROFILER_CATALOG_SNAPSHOTS,
			SeriesId::PROFILER_ENGINE_SNAPSHOTS,
			SeriesId::PROFILER_MUTATE_SNAPSHOTS,
			SeriesId::PROFILER_TRANSPORT_SNAPSHOTS,
			SeriesId::PROFILER_TASK_SNAPSHOTS,
			SeriesId::PROFILER_POLICY_SNAPSHOTS,
			SeriesId::PROFILER_FFI_SNAPSHOTS,
			SeriesId::PROFILER_CACHE_SNAPSHOTS,
			SeriesId::PROFILER_SHAPE_SNAPSHOTS,
			SeriesId::PROFILER_API_SNAPSHOTS,
			SeriesId::PROFILER_ACTOR_SNAPSHOTS,
			SeriesId::RUNTIME_MEMORY_SNAPSHOTS,
			SeriesId::RUNTIME_WATERMARKS_SNAPSHOTS,
			SeriesId::RUNTIME_OPERATORS_SNAPSHOTS,
			SeriesId::STORAGE_TABLE_SNAPSHOTS,
			SeriesId::STORAGE_VIEW_SNAPSHOTS,
			SeriesId::STORAGE_TABLE_VIRTUAL_SNAPSHOTS,
			SeriesId::STORAGE_RINGBUFFER_SNAPSHOTS,
			SeriesId::STORAGE_DICTIONARY_SNAPSHOTS,
			SeriesId::STORAGE_SERIES_SNAPSHOTS,
			SeriesId::STORAGE_FLOW_SNAPSHOTS,
			SeriesId::STORAGE_FLOW_NODE_SNAPSHOTS,
			SeriesId::STORAGE_SYSTEM_SNAPSHOTS,
			SeriesId::CDC_TABLE_SNAPSHOTS,
			SeriesId::CDC_VIEW_SNAPSHOTS,
			SeriesId::CDC_TABLE_VIRTUAL_SNAPSHOTS,
			SeriesId::CDC_RINGBUFFER_SNAPSHOTS,
			SeriesId::CDC_DICTIONARY_SNAPSHOTS,
			SeriesId::CDC_SERIES_SNAPSHOTS,
			SeriesId::CDC_FLOW_SNAPSHOTS,
			SeriesId::CDC_FLOW_NODE_SNAPSHOTS,
			SeriesId::CDC_SYSTEM_SNAPSHOTS,
		]
	}

	fn reserved_column_arrays() -> [&'static [ColumnId]; 42] {
		[
			&ColumnId::PROFILER_QUERY_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_TXN_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_STORAGE_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_PLAN_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_CDC_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_FLOW_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_SUBSCRIPTION_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_SERVER_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_WIRE_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_AUTH_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_CATALOG_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_ENGINE_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_MUTATE_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_TRANSPORT_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_TASK_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_POLICY_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_FFI_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_CACHE_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_SHAPE_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_API_SNAPSHOTS_COLUMNS,
			&ColumnId::PROFILER_ACTOR_SNAPSHOTS_COLUMNS,
			&ColumnId::RUNTIME_MEMORY_SNAPSHOTS_COLUMNS,
			&ColumnId::RUNTIME_WATERMARKS_SNAPSHOTS_COLUMNS,
			&ColumnId::RUNTIME_OPERATORS_SNAPSHOTS_COLUMNS,
			&ColumnId::STORAGE_TABLE_SNAPSHOTS_COLUMNS,
			&ColumnId::STORAGE_VIEW_SNAPSHOTS_COLUMNS,
			&ColumnId::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_COLUMNS,
			&ColumnId::STORAGE_RINGBUFFER_SNAPSHOTS_COLUMNS,
			&ColumnId::STORAGE_DICTIONARY_SNAPSHOTS_COLUMNS,
			&ColumnId::STORAGE_SERIES_SNAPSHOTS_COLUMNS,
			&ColumnId::STORAGE_FLOW_SNAPSHOTS_COLUMNS,
			&ColumnId::STORAGE_FLOW_NODE_SNAPSHOTS_COLUMNS,
			&ColumnId::STORAGE_SYSTEM_SNAPSHOTS_COLUMNS,
			&ColumnId::CDC_TABLE_SNAPSHOTS_COLUMNS,
			&ColumnId::CDC_VIEW_SNAPSHOTS_COLUMNS,
			&ColumnId::CDC_TABLE_VIRTUAL_SNAPSHOTS_COLUMNS,
			&ColumnId::CDC_RINGBUFFER_SNAPSHOTS_COLUMNS,
			&ColumnId::CDC_DICTIONARY_SNAPSHOTS_COLUMNS,
			&ColumnId::CDC_SERIES_SNAPSHOTS_COLUMNS,
			&ColumnId::CDC_FLOW_SNAPSHOTS_COLUMNS,
			&ColumnId::CDC_FLOW_NODE_SNAPSHOTS_COLUMNS,
			&ColumnId::CDC_SYSTEM_SNAPSHOTS_COLUMNS,
		]
	}

	#[test]
	fn system_series_ids_are_reserved_unique_and_below_user_range() {
		let mut seen = HashSet::new();
		assert!(seen.insert(RingBufferId::REQUEST_HISTORY.0), "ringbuffer source id setup");
		assert!(seen.insert(RingBufferId::STATEMENT_STATS.0), "ringbuffer source id setup");

		for id in reserved_series_ids() {
			assert!(id.0 < USER_ID_START, "system series id {} leaks into the user range", id.0);
			assert!(
				seen.insert(id.0),
				"system series id {} collides with another reserved source id",
				id.0
			);
		}
	}

	#[test]
	fn system_column_ids_are_reserved_unique_and_below_user_range() {
		let mut seen = HashSet::new();
		for ringbuffer_column in 1..=18u64 {
			assert!(seen.insert(ringbuffer_column), "ringbuffer column id setup");
		}

		let mut count = 0;
		for array in reserved_column_arrays() {
			for &id in array {
				assert!(id.0 < USER_ID_START, "system column id {} leaks into the user range", id.0);
				assert!(
					seen.insert(id.0),
					"system column id {} collides with another reserved column id",
					id.0
				);
				count += 1;
			}
		}

		assert_eq!(count, 21 * 22 + 3 * 5 + 9 * 13 + 9 * 7, "expected exactly 657 reserved system column ids");
	}

	#[test]
	fn snapshot_column_arrays_have_expected_widths() {
		let arrays = reserved_column_arrays();
		for array in &arrays[..21] {
			assert_eq!(array.len(), 22, "profiler snapshot series must declare 22 column ids");
		}
		for array in &arrays[21..24] {
			assert_eq!(array.len(), 5, "runtime snapshot series must declare 5 column ids");
		}
		for array in &arrays[24..33] {
			assert_eq!(array.len(), 13, "storage snapshot series must declare 13 column ids");
		}
		for array in &arrays[33..] {
			assert_eq!(array.len(), 7, "cdc snapshot series must declare 7 column ids");
		}
	}
}
