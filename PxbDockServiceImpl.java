package com.wosai.crm.side.service.pxb;


import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ReflectUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Lists;
import com.googlecode.jsonrpc4j.spring.AutoJsonRpcServiceImpl;
import com.wosai.crm.side.biz.ApolloBiz;
import com.wosai.crm.side.biz.pxb.PxbAccountSharedPoolBiz;
import com.wosai.crm.side.biz.pxb.PxbSyncDataBiz;
import com.wosai.crm.side.biz.pxb.SyncKnowledgeSystemBiz;
import com.wosai.crm.side.biz.pxb.SyncOrgBiz;
import com.wosai.crm.side.biz.pxb.SyncProjectBiz;
import com.wosai.crm.side.biz.pxb.SyncProjectMemberBiz;
import com.wosai.crm.side.biz.pxb.SyncUserBiz;
import com.wosai.crm.side.biz.pxb.SyncUserInfoBiz;
import com.wosai.crm.side.entity.PxbKnowledgeSystem;
import com.wosai.crm.side.entity.PxbProject;
import com.wosai.crm.side.entity.PxbProjectMember;
import com.wosai.crm.side.entity.PxbUserInfo;
import com.wosai.crm.side.entity.SyncUser;
import com.wosai.crm.side.enums.ProjectMemberStatusEnum;
import com.wosai.crm.side.enums.SearchProjectRangeEnum;
import com.wosai.crm.side.exception.CrmSideException;
import com.wosai.crm.side.model.PxbApis;
import com.wosai.crm.side.model.Response;
import com.wosai.crm.side.request.GetPxbProjectMemberReq;
import com.wosai.crm.side.request.GetPxbProjectsReq;
import com.wosai.crm.side.request.LearningStatusUpdateRequest;
import com.wosai.crm.side.request.ProjectInfoUpdateRequest;
import com.wosai.crm.side.request.SharedAccountCreatRequest;
import com.wosai.crm.side.response.KnowledgeSystemResp;
import com.wosai.crm.side.response.PageResult;
import com.wosai.crm.side.response.PageResultForWD;
import com.wosai.crm.side.response.PxbOrganizationResp;
import com.wosai.crm.side.response.PxbPositionResp;
import com.wosai.crm.side.response.PxbProjectMemberResp;
import com.wosai.crm.side.response.PxbProjectsResp;
import com.wosai.crm.side.service.OssService;
import com.wosai.crm.side.service.ThirdApiService;
import com.wosai.data.dao.Criteria;
import com.wosai.data.dao.Dao;
import com.wosai.data.dao.DaoConstants;
import com.wosai.data.dao.Filter;
import com.wosai.it.human.resources.middleware.model.request.DeptQueryRequest;
import com.wosai.it.human.resources.middleware.platform.DepartmentService;
import com.wosai.sales.core.model.Organization;
import com.wosai.sales.core.model.Position;
import com.wosai.sales.core.model.User;
import com.wosai.sales.core.model.UserOrganization;
import com.wosai.sales.core.service.OrganizationService;
import com.wosai.sales.core.service.PositionService;
import com.wosai.sales.core.service.UserOrganizationService;
import com.wosai.sales.core.service.UserService;
import com.wosai.sales.http.util.ExcelUtil;
import com.wosai.sales.http.util.MapsUtil;
import com.wosai.sales.push.service.DingDingService;
import com.wosai.sales.push.service.LarkService;
import com.wosai.upay.common.bean.PageInfo;
import com.wosai.upay.common.dao.PageInfoUtil;
import java.util.function.Consumer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import vo.ApiRequestParam;
import vo.UserVo;

import java.util.*;
import java.util.stream.Collectors;

import static com.wosai.data.util.CollectionUtil.hashMap;

@Slf4j
@Service
@AutoJsonRpcServiceImpl
@RequiredArgsConstructor
public class PxbDockServiceImpl implements PxbDockServer {
    private final ThirdApiService pxbThirdApi;
    private final OrganizationService organizationService;
    private final UserService userService;
    private final UserOrganizationService userOrganizationService;
    private final SyncOrgBiz syncOrgBiz;
    private final SyncUserBiz syncUserBiz;
    private final PxbSyncDataBiz pxbSyncDataBiz;
    private final DingDingService dingDingService;
    private final PositionService positionService;
    private final ApolloBiz apolloBiz;
    private final ApplicationContext context;
    private final PxbAccountSharedPoolBiz pxbAccountSharedPoolBiz;
    private final DepartmentService departmentService;
    private final OssService ossService;
    private final LarkService larkService;
    private final SyncProjectBiz syncProjectBiz;
    private final SyncProjectMemberBiz syncProjectMemberBiz;
    private final SyncKnowledgeSystemBiz syncKnowledgeSystemBiz;
    private final SyncUserInfoBiz syncUserInfoBiz;
    private final Dao<Map<String, Object>> pxbProjectDao;
    private final Dao<Map<String, Object>> pxbProjectMemberDao;

    @Value("${third_param.pxb.root_org}")
    private String pxbRootOrganizationCode;

    @Value("${third_param.pxb.deleted_user_org}")
    private String deletedUserOrg;

    @Override
    public String getPxbLoginToken(ApiRequestParam<String, HashMap> request) {
        UserVo user = request.getUser();
        String userId = user.getId();
        String userAccount = user.getCode();
        String organizationNames = user.getOrganizationNames();
        if (!this.checkUserHasPxbAccount(userId) && StringUtils.startsWith(organizationNames, "服务商")) {
            String[] split = StringUtils.split(organizationNames, "/");
            if (split.length < 2) {
                throw new CrmSideException("用户没有二级渠道无法获取共享账号: " + user.getCode());
            }
            String channel = split[1];
            userAccount = pxbAccountSharedPoolBiz.getAccountFromSharedPoolByChannel(channel);
        }
        String redirectUrl = request.getBodyParams();
        String pxbRedirectUrl = apolloBiz.getString("pxb_redirect_url", "https://m.taoke.com/s/A36974");
        Response<String> response = pxbThirdApi.execute(PxbApis.GET_PXB_LOGIN_TOKEN.addBody(hashMap(
                "code", userAccount,
                "redirect", StringUtils.isNotBlank(redirectUrl) ? redirectUrl : pxbRedirectUrl
        )));
        return response.getData();
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void syncOrganization(List<String> orgIds) {
        syncOrganization(orgIds, false);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void syncOrganization(List<String> orgIds, boolean isCreate) {
        List<Map> body = Lists.newArrayList();
        for (String id : orgIds) {
            if (!checkOrgInPxb(id)) {
                if (!isCreate) {
                    continue;
                }
            }

            Map simpleOrganization = organizationService.getSimpleOrganization(id);
            if (MapUtils.isEmpty(simpleOrganization)) {
                throw new CrmSideException("相关组织不存在: "+id);
            }
            String path = MapUtils.getString(simpleOrganization, Organization.PATH);
            String[] pathArray = StringUtils.split(path, ",");
            for (int i = 0; i < pathArray.length; i++) {
                String subCode = pathArray[i];
                Map parentOrg = organizationService.getSimpleOrganizationByCode(subCode);
                if (MapUtils.isEmpty(parentOrg)) {
                    throw new CrmSideException("相关组织不存在: "+subCode);
                }
                syncOrgBiz.saveOrUpdate(
                        MapUtils.getString(parentOrg, DaoConstants.ID),
                        MapUtils.getString(parentOrg, Organization.PARENT)
                );
                body.add(hashMap(
                        "code", subCode,
                        "organization_name", MapUtils.getString(parentOrg, Organization.NAME),
                        "parent_code", i == 0 ? pxbRootOrganizationCode : pathArray[i-1]
                ));
            }
        }
        List<List<Map>> partition = Lists.partition(body, 1000);
        for (List<Map> list : partition) {
            pxbThirdApi.execute(PxbApis.SYNC_ORGANIZATION.addBody(list));
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void syncSharedAccount(List<SharedAccountCreatRequest> requests) {
        List<Map> sharedAccounts = Lists.newArrayList();
        for (SharedAccountCreatRequest req : requests) {
            String account = req.getAccount();
            String name = req.getName();
            String channel = req.getChannel();
            String organizationCode = req.getOrganizationCode();
            Map simpleOrg = organizationService.getSimpleOrganizationByCode(organizationCode);
            if (MapUtils.isEmpty(simpleOrg)) {
                throw new CrmSideException("此组织不存在: " + organizationCode);
            }
            syncOrganization(Lists.newArrayList(MapUtils.getString(simpleOrg, DaoConstants.ID)), true);
            pxbAccountSharedPoolBiz.createSharedAccount(account, channel);
            sharedAccounts.add(hashMap(
                    "code",account,
                    "chinese_name", name,
                    "employee_id", account,
                    "elearning",1,
                    "default_code",organizationCode,
                    "organization_codes",Lists.newArrayList(organizationCode)
            ));
        }
        List<List<Map>> partition = Lists.partition(sharedAccounts, 1000);
        for (List<Map> body : partition) {
            pxbThirdApi.execute(PxbApis.SYNC_STUDENT.addBody(body));
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void syncUser(List<String> userIds) {
        syncUser(userIds, false);
    }

    @Override
    @SneakyThrows
    public void createAccountForNewEmployee(String dateStr) {
        Map<String, List<String>> autoCreatePxbAccountOrg = JSON.parseObject(apolloBiz.getString("auto_create_pxb_account_org", "{}"), new TypeReference<Map<String, List<String>>>() {});
        List<String> feishuOrgList = autoCreatePxbAccountOrg.get("feishu_org_list");
        List<String> crmOrgList = autoCreatePxbAccountOrg.get("crm_org_list");
        Date currentDate = DateUtil.date();
        Date today = StringUtils.isNotBlank(dateStr) ? DateUtil.parseDate(dateStr) : currentDate;
        Date startDate = StringUtils.isNotBlank(dateStr) ? DateUtil.parseDate(dateStr) : getStartDateOfSearch(currentDate);
        // 查出新入职的用户
        DeptQueryRequest deptQueryRequest = new DeptQueryRequest();
        deptQueryRequest.setDeptCodeList(feishuOrgList);
        deptQueryRequest.setHireTimeStart(DateUtil.formatDateTime(DateUtil.beginOfDay(startDate)));
        deptQueryRequest.setHireTimeEnd(DateUtil.formatDateTime(DateUtil.endOfDay(today)));
        JSONObject userData = departmentService.getDeptTreeUsersByCode(deptQueryRequest);
        JSONArray ssoUserList = userData.getJSONArray("data");
        List<String> userIds = ssoUserList.stream().map(obj -> {
            if (!(obj instanceof Map)) {
                return null;
            }
            Map<String, String> userMap = (Map<String, String>) obj;
            String jobNumber = userMap.get("jobNumber");
            if (StringUtils.isBlank(jobNumber)) {
                return null;
            }
            Map<String, Object> simpleUserBySSOId = userService.getSimpleUserBySSOId(jobNumber);
            if (MapUtils.isEmpty(simpleUserBySSOId)) {
                return null;
            }
            String userId = MapUtils.getString(simpleUserBySSOId, DaoConstants.ID);
            String organizationId = MapUtils.getString(simpleUserBySSOId, User.ORGANIZATION_ID);
            Map<String, Object> simpleOrganization = organizationService.getSimpleOrganization(organizationId);
            String path = MapUtils.getString(simpleOrganization, Organization.PATH);
            return crmOrgList.stream().anyMatch(path::startsWith) ? userId : null;
        }).filter(Objects::nonNull)
            .filter(userId -> !checkUserHasPxbAccount(userId)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(userIds)) {
            return;
        }
        List<Map<String, Object>> userInfoList = userIds.stream()
                .map(userId -> {
                    Map<String, Object> user = userService.getUser(userId);
                    user.put("today", DateUtil.formatDate(today));
                    try {
                        context.getBean(PxbDockServiceImpl.class).syncUser(Lists.newArrayList(userId), true);
                        user.put("pxb_open_status", "开通成功");
                    } catch (Exception e) {
                        log.warn("开通pxb账号失败: ", e);
                        user.put("pxb_open_status", "开通失败");
                    }
                    return user;
                })
                .collect(Collectors.toList());
        byte[] openPxbAccountExcel = ExcelUtil.mapToExcel(
                Lists.newArrayList("组织", "岗位", "学员姓名", "用户编码", "开通状态", "开通日期"),
                Lists.newArrayList(User.ORGANIZATION_NAMES, User.POSITION_NAME, User.LINKMAN, User.CODE, "pxb_open_status", "today"),
                userInfoList
        );
        String name = String.format("研学社账号开通列表(%s).xlsx", DatePattern.CHINESE_DATE_FORMAT.format(today));
        String url = ossService.upload(name, openPxbAccountExcel);
        Map<String, String> autoOpenPxbAccountNoticeFeishu = JSON.parseObject(apolloBiz.getString("auto_open_pxb_account_notice_feishu", "{}"), new TypeReference<Map<String, String>>() {});
        String token = autoOpenPxbAccountNoticeFeishu.get("token");
        String secret = autoOpenPxbAccountNoticeFeishu.get("secret");
        larkService.sendLarkSms(token, url, secret);
    }

    /**
     * 从apollo中获取配置 - 几天内入职的新员工
     * @return
     */
    private Date getStartDateOfSearch(Date date) {
        int daysRange = Math.toIntExact(apolloBiz.getLong("create_pxb_account_in_search_date_range", 7L));
        int actualDaysRange = daysRange > 0 ? daysRange - 1 : 6;
        return DateUtil.offsetDay(date, -actualDaysRange);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void syncUser(List<String> userIds, boolean isCreate) {
        List<Map> students = Lists.newArrayList();
        List<Map<String, Object>> userByIds = userService.findUserByIds(userIds);
        for (Map<String, Object> simpleUser : userByIds) {
            String userId = MapUtils.getString(simpleUser, DaoConstants.ID);
            // 查询该用户是否在同步用户表中
            if (!checkUserHasPxbAccount(userId) && !isCreate) {
                continue;
            }
            String userName = MapUtils.getString(simpleUser, User.LINKMAN);
            if (User.STATUS_DISABLED == MapUtils.getIntValue(simpleUser, User.STATUS)
                    || MapUtils.getBooleanValue(simpleUser, DaoConstants.DELETED)) {
                throw new CrmSideException(String.format("用户已离职:【%s】！", userName));
            }
            // 查询该用户多组织关系，获取方法已按时间排序，第一个即为默认组织
            List<Map<String, Object>> userOrganizationById = (List<Map<String, Object>>) userOrganizationService.findUserOrganizationById(hashMap(UserOrganization.USER_ID, userId));
            if (CollectionUtils.isEmpty(userOrganizationById)) {
                throw new CrmSideException(String.format("用户无组织:【%s】！", userName));
            }
            // 默认组织
            Map<String, Object> defaultUserOrg = userOrganizationById.get(0);
            String defaultOrgId = MapUtils.getString(defaultUserOrg, UserOrganization.ORGANIZATION_ID);
            Map<String, Object> defaultOrg = organizationService.getSimpleOrganization(defaultOrgId);
            String defaultCode = MapUtils.getString(defaultOrg, Organization.CODE);
            String positionId = MapUtils.getString(defaultUserOrg, UserOrganization.POSITION_ID);
            syncPosition(positionId, defaultCode);
            syncUserBiz.saveOrUpdate(userId, defaultOrgId);

            // 通过多组织关系获取组织code，同时获取时进行同步组织操作
            List<String> userHasOrgIds = userOrganizationById.stream()
                    .map(userOrgMap -> MapUtils.getString(userOrgMap, UserOrganization.ORGANIZATION_ID))
                    .collect(Collectors.toList());
            syncOrganization(userHasOrgIds, true);
            List<String> organizationCodes = userHasOrgIds.stream().map(orgId -> {
                Map<String, Object> simpleOrganization = organizationService.getSimpleOrganization(orgId);
                return MapUtils.getString(simpleOrganization, Organization.CODE);
            }).collect(Collectors.toList());

            students.add(hashMap(
                    "code",MapUtils.getString(simpleUser, User.CODE),
                    "chinese_name", userName,
                    "employee_id", MapUtils.getString(simpleUser, User.CODE),
                    "elearning",1,
                    "level_codes", Lists.newArrayList(positionId),
                    "default_code",defaultCode,
                    "organization_codes",organizationCodes
            ));
        }
        List<List<Map>> partition = Lists.partition(students, 1000);
        for (List<Map> body : partition) {
            pxbThirdApi.execute(PxbApis.SYNC_STUDENT.addBody(body));
        }
        List<String> codes = students.stream().map(data -> MapUtils.getString(data, User.CODE)).collect(Collectors.toList());
        syncUserInfo(codes);
    }

    private void syncPosition(String positionId, String code) {
        String errMsg = "岗位不存在:【" + positionId + "】!";
        if (code != null) {
            errMsg = "用户岗位异常:【"+code+"】"+errMsg;
        }
        Assert.isTrue(StringUtils.isNotBlank(positionId), errMsg);
        Map position = positionService.getPositionDetail(positionId);
        Assert.isTrue(MapUtils.isNotEmpty(position), errMsg);
        String name = MapUtils.getString(position, Position.NAME);
        pxbThirdApi.execute(PxbApis.SYNC_TEAM.addBody(Lists.newArrayList(hashMap(
                "code", positionId,
                "name", name
        ))));
        pxbThirdApi.execute(PxbApis.SYNC_LEVEL.addBody(Lists.newArrayList(hashMap(
                "code", positionId,
                "name", name,
                "talentteam_code", positionId
        ))));
    }

    @Override
    public void syncPosition(String positionId) {
        syncPosition(positionId, null);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void deleteOrg(List<String> orgIds) {
        List<String> pendingDealIds = orgIds.stream().filter(this::checkOrgInPxb).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(pendingDealIds)) {
            return;
        }
        List<String> pendingDeleteOrgIds = pendingDealIds.stream()
                .map(syncOrgBiz::getAllSubOrgAsList)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        List<String> pendingDeleteUserIds = syncUserBiz.getSyncUserByOrgIds(pendingDeleteOrgIds);
        this.deleteUser(pendingDeleteUserIds);
        List<String> orgCodes = pendingDeleteOrgIds.stream()
                .map(id -> MapUtils.getString(organizationService.getSimpleOrganization(id), Organization.CODE))
                .collect(Collectors.toList());
        List<List<String>> partition = Lists.partition(orgCodes, 1);
        for (List<String> body : partition) {
            pxbThirdApi.execute(PxbApis.DELETE_ORGANIZATION.addBody(body));
        }
        for (String orgId : pendingDealIds) {
            syncOrgBiz.deleteOrg(orgId);
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void deleteUser(List<String> userIds) {
        List<String> pendingDealIds = userIds.stream().filter(this::checkUserHasPxbAccount).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(pendingDealIds)) {
            return;
        }
        List<Map> students = Lists.newArrayList();
        for (String userId : pendingDealIds) {
            Map simpleUser = userService.getSimpleUser(userId);
            students.add(hashMap(
                    "code",MapUtils.getString(simpleUser, User.CODE),
                    "chinese_name",MapUtils.getString(simpleUser, User.LINKMAN),
                    "employee_id", MapUtils.getString(simpleUser, User.CODE),
                    "elearning", 0,
                    "default_code",deletedUserOrg,
                    "organization_codes",Lists.newArrayList(deletedUserOrg)
            ));
            syncUserBiz.deleteUser(userId);
        }
        List<List<Map>> partition = Lists.partition(students, 1000);
        for (List<Map> body : partition) {
            pxbThirdApi.execute(PxbApis.SYNC_STUDENT.addBody(body));
        }
        List<String> codes = students.stream().map(data -> MapUtils.getString(data, User.CODE)).collect(Collectors.toList());
        syncUserInfo(codes);
    }

    @Override
    public boolean checkUserHasPxbAccount(String userId) {
        return syncUserBiz.checkUserHasPxbAccount(userId);
    }

    @Override
    public boolean checkOrgInPxb(String orgId) {
        return syncOrgBiz.checkOrgInPxb(orgId);
    }

    @Override
    @SneakyThrows
    public void syncPxbDataDefaultYesterday(String dateStr) {
        Date date;
        Calendar instance = Calendar.getInstance();
        instance.set(Calendar.HOUR_OF_DAY, 0);
        instance.set(Calendar.MINUTE, 0);
        instance.set(Calendar.SECOND, 0);
        instance.set(Calendar.MILLISECOND, 0);
        if (StringUtils.isBlank(dateStr) || dateStr.length() != 8) {
            instance.add(Calendar.DAY_OF_MONTH, -1);
        } else {
            int year = Integer.parseInt(dateStr.substring(0, 4));
            int month = Integer.parseInt(dateStr.substring(4, 6));
            int day = Integer.parseInt(dateStr.substring(6, 8));
            instance.set(Calendar.YEAR, year);
            instance.set(Calendar.MONTH, month-1);
            instance.set(Calendar.DAY_OF_MONTH, day);
        }
        date = instance.getTime();
        try {
            syncProjectsIncrement(date);
            syncProjectsStudentIncrement(date);
            syncCoursesIncrement(date);
            syncCoursesStudentIncrement(date);
            syncExamsIncrement(date);
            syncExamsStudentIncrement(date);
            syncEvaluationsIncrement(date);
            syncEvaluationsStudentIncrement(date);
            syncSignsIncrement(date);
            syncSignsStudentIncrement(date);
            syncSignupsIncrement(date);
            syncSignupsStudentIncrement(date);
            syncEmployeesStudentPointIncrement(date);
        } catch (Exception e) {
            Map<String, String> syncPxbDataFailureNoticeFeishu = JSON.parseObject(apolloBiz.getString("sync_pxb_data_failure_notice", "{}"), new TypeReference<Map<String, String>>() {});
            String token = syncPxbDataFailureNoticeFeishu.get("token");
            String secret = syncPxbDataFailureNoticeFeishu.get("secret");
            larkService.sendLarkSms(token, String.format("同步培训宝数据失败：%s", e.getMessage()), secret);
            log.error("日期:{}, 同步培训宝数据失败: ", DateUtil.formatDate(date), e);
            throw e;
        }
    }

    @Override
    public void syncProjectsIncrement(Date date) {
        String tableName = "pxb_projects_increment";
        List<String> syncColumns = Lists.newArrayList("pxb_id","subject","knowledge_ids","mobile_url","address","budget_cost","reality_cost","begin_time","end_time","create_time","update_time","disabled","not_required_open_team_range","not_required_open_org_range","not_required_open_type","required_open_team_range","required_open_org_range","required_open_type");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.PROJECTS_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }
    @Override
    public void syncProjectsStudentIncrement(Date date) {
        String tableName = "pxb_projects_member_increment";
        List<String> syncColumns = Lists.newArrayList("pxb_id","project_id","code","chinese_name","employee_id","mobile","email","join_time","status","finish_progress","required_type");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.PROJECTS_STUDENT_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }
    @Override
    public void syncCoursesIncrement(Date date) {
        String tableName = "pxb_courses_increment";
        List<String> syncColumns = Lists.newArrayList("pxb_id","project_id","subject","course_type","knowledge_ids","mobile_url","course_duration","course_category","trainer_name","trainer_id","address","begin_time","end_time","create_time","update_time","disabled");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.COURSES_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }
    @Override
    public void syncCoursesStudentIncrement(Date date) {
        String tableName = "pxb_courses_member_increment";
        List<String> syncColumns = Lists.newArrayList("pxb_id","code","chinese_name","employee_id","mobile","email","project_id","course_type","learn_type","finish_time");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.COURSES_STUDENT_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }
    @Override
    public void syncExamsIncrement(Date date) {
        String tableName = "pxb_exams_increment";
        List<String> syncColumns = Lists.newArrayList("pxb_id","event_type","event_id","project_id","subject","begin_time","end_time","link_course","create_time","update_time","disabled","mobile_url");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.EXAMS_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }
    @Override
    public void syncExamsStudentIncrement(Date date) {
        String tableName = "pxb_exams_member_increment";
        List<String> syncColumns = Lists.newArrayList("pxb_id","code","chinese_name","employee_id","mobile","email","project_id","result","result_status","score","update_time");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.EXAMS_STUDENT_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }
    @Override
    public void syncEvaluationsIncrement(Date date) {
        String tableName = "pxb_evaluations_increment";
        List<String> syncColumns = Lists.newArrayList("pxb_id","project_id","subject","begin_time","end_time","link_course","course_type","create_time","update_time","disabled","mobile_url");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.EVALUATIONS_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }
    @Override
    public void syncEvaluationsStudentIncrement(Date date) {
        String tableName = "pxb_evaluations_member_increment";
        List<String> syncColumns = Lists.newArrayList("pxb_id","code","chinese_name","employee_id","mobile","email","project_id","result","update_time");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.EVALUATIONS_STUDENT_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }
    @Override
    public void syncSignsIncrement(Date date) {
        String tableName = "pxb_signs_increment";
        List<String> syncColumns = Lists.newArrayList("pxb_id","project_id","subject","course_before_begin_time","course_before_end_time","course_after_begin_time","course_after_end_time","sign_duration","link_course","course_type","create_time","update_time","enabled","is_delete","mobile_url");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.SIGNS_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }
    @Override
    public void syncSignsStudentIncrement(Date date) {
        String tableName = "pxb_signs_member_increment";
        List<String> syncColumns = Lists.newArrayList("pxb_id","code","chinese_name","employee_id","mobile","email","project_id","result","update_time");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.SIGNS_STUDENT_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }
    @Override
    public void syncSignupsIncrement(Date date) {
        String tableName = "pxb_signups_increment";
        List<String> syncColumns = Lists.newArrayList("pxb_id","project_id","subject","begin_time","end_time","link_course","course_type","create_time","update_time","is_delete","mobile_url");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.SIGNUPS_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }
    @Override
    public void syncSignupsStudentIncrement(Date date) {
        String tableName = "pxb_signups_member_increment";
        List<String> syncColumns = Lists.newArrayList("pxb_id","code","chinese_name","employee_id","mobile","email","project_id","result","update_time");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.SIGNUPS_STUDENT_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }
    @Override
    public void syncEmployeesStudentPointIncrement(Date date) {
        String tableName = "pxb_employees_point_increment";
        List<String> syncColumns = Lists.newArrayList("record_id","code","chinese_name","employee_id","mobile","email","time","score","action");
        List<Map<String, Object>> result = requestForPxbData(PxbApis.EMPLOYEES_STUDENT_POINT_INCREMENT, date);
        pxbSyncDataBiz.batchSave(tableName, syncColumns, result, date);
    }

    @Override
    public void syncDataBetweenDate(Date begin, Date end, String method) {
        begin = DateUtil.truncate(begin, DateField.DAY_OF_MONTH);
        end = DateUtil.truncate(end, DateField.DAY_OF_MONTH);
        PxbDockServiceImpl service = context.getBean(PxbDockServiceImpl.class);
        List<DateTime> dateList = DateUtil.rangeToList(begin, end, DateField.DAY_OF_MONTH);
        for (DateTime date : dateList) {
            ReflectUtil.invoke(service, method, date);
        }
    }

    @Override
    public void syncUserInfo(List<String> codes) {
        if (codes == null || codes.isEmpty()) {
            return;
        }

        try {
            List<Map<String, Object>> params = codes.stream().map(code -> {
                Map<String, Object> user = userService.getUserByCode(code);
                Map<String, Object> syncUser = syncUserBiz.getSyncUser(MapUtils.getString(user, DaoConstants.ID, ""));
                Map<String, Object> param = MapUtil.ofEntries(
                    MapUtil.entry(PxbUserInfo.USER_ID, MapUtils.getString(syncUser, SyncUser.USER_ID, "")),
                    MapUtil.entry(PxbUserInfo.CODE, code),
                    MapUtil.entry(PxbUserInfo.DELETED, MapUtils.getObject(syncUser, DaoConstants.DELETED, 1)));
                syncUserInfoBiz.setCrmInfo(user, param);
                return param;
            }).filter(data -> Strings.isNotBlank((String) data.get(SyncUser.USER_ID))).collect(Collectors.toList());

            batchSyncUserInfo(params);
        } catch (Exception e) {
            log.error(String.format("同步pxb_user_info失败，失败原因为：%s", e.getMessage()), e);
        }
    }

    private void batchSyncUserInfo(List<Map<String, Object>> params) {
        List<Map<String, Object>> data = Lists.newArrayList();
        for (Map<String, Object> param : params) {
            if (Strings.isNotBlank((String) param.get(PxbUserInfo.CODE))) {
                try {
                    Response<List<Map<String, Object>>> result =
                        pxbThirdApi.execute(PxbApis.GET_USER_INFO.addBody(Collections.singletonList((String) param.get(PxbUserInfo.CODE))));
                    if ("0".equals(result.getCode()) && result.getData() != null) {
                        result
                            .getData()
                            .stream()
                            .peek(value -> {
                                value.put(PxbUserInfo.ORGANIZATION_CODES, value.get(PxbUserInfo.ORGANIZATION_CODES).toString());
                                value.put(PxbUserInfo.ORGANIZATION_URLS, value.get(PxbUserInfo.ORGANIZATION_URLS).toString());
                                value.put(PxbUserInfo.USER_ID, param.get(PxbUserInfo.USER_ID));
                                value.put(PxbUserInfo.DELETED, param.get(PxbUserInfo.DELETED));
                                value.put(PxbUserInfo.POSITION_ID, param.get(PxbUserInfo.POSITION_ID));
                                value.put(PxbUserInfo.POSITION_CHANGE_TIME, param.get(PxbUserInfo.POSITION_CHANGE_TIME));
                                value.put(PxbUserInfo.CRM_APPROVE_TIME, param.get(PxbUserInfo.CRM_APPROVE_TIME));
                                value.put(PxbUserInfo.DEFAULT_ORGANIZATION_PATH, param.get(PxbUserInfo.DEFAULT_ORGANIZATION_PATH));

                            })
                            .filter(value ->
                                Strings.isNotBlank((String) value.get(PxbUserInfo.USER_ID))
                                    && Strings.isNotBlank((String) value.get(PxbUserInfo.CODE)))
                            .findFirst()
                            .ifPresent(data::add);
                    }
                } catch (Exception e) {
                    log.error(String.format("查询code为[%s]的员工信息出错，错误为: %s", param.get(PxbUserInfo.CODE), e.getMessage()), e);
                }
            }
        }
        List<String> syncColumns =
            Arrays.asList(
                PxbUserInfo.USER_ID, PxbUserInfo.CODE, PxbUserInfo.CHINESE_NAME,
                PxbUserInfo.ENGLISH_NAME, PxbUserInfo.EMPLOYEE_ID, PxbUserInfo.DEPARTMENT,
                PxbUserInfo.POSITION, PxbUserInfo.GENDER, PxbUserInfo.MANAGEMENT_LEVEL,
                PxbUserInfo.TECHNICAL_LEVEL, PxbUserInfo.EMAIL, PxbUserInfo.MOBILE,
                PxbUserInfo.TELEPHONE, PxbUserInfo.WEIXIN, PxbUserInfo.QQ,
                PxbUserInfo.PASSWORD, PxbUserInfo.TAGS, PxbUserInfo.EMPLOYMENT_DATE,
                PxbUserInfo.ELEARNING, PxbUserInfo.DEFAULT_CODE,
                PxbUserInfo.ORGANIZATION_CODES, PxbUserInfo.ORGANIZATION_URLS, PxbUserInfo.DELETED,
                PxbUserInfo.POSITION_ID, PxbUserInfo.POSITION_CHANGE_TIME, PxbUserInfo.CRM_APPROVE_TIME, PxbUserInfo.DEFAULT_ORGANIZATION_PATH);
        for (List<Map<String, Object>> partitionData : Lists.partition(data, 50)) {
            pxbSyncDataBiz.batchSave("pxb_user_info", syncColumns, partitionData, PxbUserInfo.CODE);
        }
    }

    @Override
    public void syncAllUserInfo() {
        int page = 1;
        List<Map<String, Object>> syncUsers = syncUserBiz.getSyncUsers(new PageInfo(page, 1000));
        while (syncUsers != null && !syncUsers.isEmpty()) {
            List<String> userIds =
                syncUsers.stream().map(data -> MapUtils.getString(data, SyncUser.USER_ID, "")).collect(Collectors.toList());
            List<Map<String, Object>> params =
                syncUsers
                    .stream()
                    .map(data -> MapUtil.ofEntries(
                        MapUtil.entry(PxbUserInfo.USER_ID, MapUtils.getString(data, SyncUser.USER_ID, "")),
                        MapUtil.entry(PxbUserInfo.DELETED, MapUtils.getObject(data, DaoConstants.DELETED, 1))))
                    .collect(Collectors.toList());

            for (List<String> partitionUserIds : Lists.partition(userIds, 50)) {
                List<Map<String, Object>> users = userService.findUserByIds(partitionUserIds);
                params.forEach(param -> {
                    for(Map<String, Object> user : users) {
                        if (param.get(PxbUserInfo.USER_ID).equals(user.get(DaoConstants.ID))) {
                            param.put(PxbUserInfo.CODE, MapUtils.getString(user, User.CODE, ""));
                            syncUserInfoBiz.setCrmInfo(user, param);
                        }
                    }
                });
            }
            batchSyncUserInfo(params);
            page++;
            syncUsers = syncUserBiz.getSyncUsers(new PageInfo(page, 1000));
        }
    }

    private List<Map<String,Object>> requestForPxbData(PxbApis<Map<String, Object>> api, Date date) {
        Calendar instance = Calendar.getInstance();
        instance.setTime(date);
        instance.set(Calendar.HOUR_OF_DAY, 0);
        instance.set(Calendar.MINUTE, 0);
        instance.set(Calendar.SECOND, 0);
        instance.set(Calendar.MILLISECOND, 0);
        long beginTimestamp = instance.getTime().getTime() / 1000;
        instance.add(Calendar.DAY_OF_MONTH, 1);
        long endTimestamp = instance.getTime().getTime() / 1000;
        Map<String, Long> body = new HashMap<String, Long>() {{
            put("begin_time", beginTimestamp);
            put("end_time", endTimestamp);
            put("start", 1L);
            put("count", 100L);
            put("is_all", 1L);
        }};
        List<Map<String,Object>> dataList = Lists.newArrayList();
        while (true) {
            Response<Map<String, Object>> response = pxbThirdApi.execute(api.addBody(body));
            Map<String, Object> data = response.getData();
            Object listObj = MapUtils.getObject(data, "list");
            if (!(listObj instanceof List)) {
                return dataList;
            }
            List<Map<String, Object>> list = (List<Map<String, Object>>) listObj;

            List<Map<String, Object>> xhList = formateList(list);
            dataList.addAll(xhList);
            long next = MapUtils.getLongValue(data, "next");
            if (Objects.equals(next, 0L)) {
                break;
            }
            body.put("start", next);
        }
        return dataList;
    }

    /**
     * 将map中所有驼峰命名的key转成下划线命名
     * @param list
     * @return
     */
    private List<Map<String, Object>> formateList(List<Map<String, Object>> list) {
        List<Map<String, Object>> xhList = Lists.newArrayList();
        for (Map<String, Object> map : list) {
            Map<String, Object> nMap = new HashMap<>();
            for (String key : map.keySet()) {
                Object value = map.get(key);
                // 处理pxb接口数据到数据库的特殊情况
                if (value instanceof Collection) {
                    Collection<String> collection = (Collection<String>) value;
                    value = "["+StringUtils.join(collection, ",")+"]";
                }else if (StringUtils.endsWithIgnoreCase(key, "time")) {
                    try {
                        long time = Long.parseLong(value.toString());
                        if (time != 0) {
                            value = new Date(time * 1000);
                        } else {
                            value = null;
                        }
                    } catch (Exception e) {
                        value = null;
                    }
                }else if (StringUtils.isBlank(Objects.toString(value))) {
                    value = null;
                }
                if (StringUtils.equals(key, "id")) {
                    key = "pxb_id";
                }
                if (StringUtils.equals(key, "is_required")) {
                    key = "required_type";
                }
                nMap.put(convertToUnderscore(key), value);
            }
            xhList.add(nMap);
        }
        return xhList;
    }

    /**
     * 驼峰转下划线
     * @param str 驼峰string
     * @return 下划线string
     */
    private static String convertToUnderscore(String str) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (Character.isUpperCase(c)) {
                sb.append("_").append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * 同步培训宝知识体系表
     */
    public void syncKnowledgeSystem() {
        try {
            Response<List<Map<String, Object>>> result = pxbThirdApi.execute(PxbApis.GET_KNOWLEDGES);;
            if (result.getData() != null) {
                List<Map<String, Object>> dataList = result.getData().stream().peek(value -> {
                    value.put(PxbKnowledgeSystem.PXB_ID, value.get("id"));
                    value.put(PxbKnowledgeSystem.PXB_PARENT_ID, value.get("parent_id"));
                    value.remove("update_time");
                    value.remove("create_time");
                    value.remove("id");
                    value.remove("parent_id");
                }).collect(Collectors.toList());
                for (Map<String, Object> data: dataList) {
                    syncKnowledgeSystemBiz.saveAndUpdateKnowledgeSystem(data);
                }
            }
        } catch (Exception e) {
            log.error(String.format("同步知识体系出错，错误为: %s", e.getMessage()), e);
        }
    }

    @Override
    public void updateMemberLearningStatus(LearningStatusUpdateRequest learningStatusUpdateRequest) {
        // 校验
        if(!syncProjectMemberBiz.generateSignature(learningStatusUpdateRequest).equals(learningStatusUpdateRequest.getSignature())) {
            throw new CrmSideException("参数被篡改，请重新填写并提交");
        }
        Map<String, Object> projectMember = syncProjectMemberBiz.updateMemberLearningStatus(learningStatusUpdateRequest);
        if (Objects.isNull(projectMember)) {
            log.error("项目成员不存在，请检查后重新调用，project_id为 {}, 成员编码为 {}，成员姓名为 {}",
                learningStatusUpdateRequest.getProjectId(),
                learningStatusUpdateRequest.getCode(),
                learningStatusUpdateRequest.getChineseName());
            throw new CrmSideException(
                String.format(
                    "项目成员不存在，请检查后重新调用，project_id为 %s, 成员编码为 %s，成员姓名为 %s",
                    learningStatusUpdateRequest.getProjectId(),
                    learningStatusUpdateRequest.getCode(),
                    learningStatusUpdateRequest.getChineseName()));
        }
    }

    @Override
    public Map<String, String> syncPxbProjects(ApiRequestParam<String, HashMap> request) {
        Date beginDate = syncProjectBiz.getLastSyncProjectTime();
        syncPxbProjectAndMemberInfo(beginDate, Calendar.getInstance().getTime());
        return MapsUtil.hashMap("status", "success");
    }

    @Override
    public String getLastSyncPxbProjectTime(ApiRequestParam<String, HashMap> request) {
        Date lastSyncProjectTime = syncProjectBiz.getLastSyncProjectTime();
        return com.wosai.crm.side.util.DateUtil.getSdf(com.wosai.crm.side.util.DateUtil.FMT_2).format(lastSyncProjectTime);
    }

    @Override
    public void syncPxbProjectAndMemberInfo(Date begin, Date end) {
        syncKnowledgeSystem();
        syncInfoAndSave(PxbApis.PROJECTS_INCREMENT, begin, end, syncProjectBiz::saveAndUpdateProject);
        syncProjectMember(begin);
    }

    @Override
    public void syncAllPxbProjectMember() {
        syncProjectMember(null);
    }

    @Override
    public void syncPxbProjectAndMemberInfoByDateStr(String beginDateStr, String endDateStr) {
        LocalDateTime endTime =
            Strings.isBlank(endDateStr)
                ? LocalDateTime.now()
                : LocalDateTime.parse(endDateStr, com.wosai.crm.side.util.DateUtil.getDtf(com.wosai.crm.side.util.DateUtil.FMT_2));
        LocalDateTime startTime =
            Strings.isBlank(beginDateStr)
                ? endTime.minusDays(1)
                : LocalDateTime.parse(beginDateStr, com.wosai.crm.side.util.DateUtil.getDtf(com.wosai.crm.side.util.DateUtil.FMT_2));

        ZoneId zone = ZoneId.systemDefault();
        Date startDate = Date.from(startTime.atZone(zone).toInstant());
        Date endDate = Date.from(endTime.atZone(zone).toInstant());

        syncPxbProjectAndMemberInfo(startDate, endDate);
    }

    @Override
    public List<KnowledgeSystemResp> getPxbKnowledgeSystem(ApiRequestParam<HashMap, HashMap> request) {
        List<KnowledgeSystemResp> knowledgeSystemResp = syncKnowledgeSystemBiz.getKnowledgeSystemResp();
        String type = (String) request.getBodyParams().get("type");
        if ("all".equals(type)) {
            return knowledgeSystemResp;
        }
        List<Long> organizationKnowledgeSystemIds = syncKnowledgeSystemBiz.getOrganizationKnowledgeSystemIds(request.getUser().getOrganizationPath(), request.getUser().isManager());
        return
            knowledgeSystemResp
                .stream()
                .filter(result -> organizationKnowledgeSystemIds.contains(result.getKnowledgeSystemId()))
                .collect(Collectors.toList());
    }

    @Override
    public List<KnowledgeSystemResp> getSelfPxbKnowledgeSystem(ApiRequestParam<HashMap, HashMap> request) {
        Map<String, Object> userInfo = syncUserInfoBiz.getUserInfo(request.getUser().getId());
        if (Objects.isNull(userInfo)) {
            return Lists.newArrayList();
        }
        GetPxbProjectsReq params = new GetPxbProjectsReq();
        params.setPage(1);
        params.setPageSize(10);
        params.setMemberCode(request.getUser().getCode());
        return
            syncKnowledgeSystemBiz
                .getSelfPxbKnowledgeSystem(userInfo)
                .stream()
                .filter(knowledgeSystemResp -> {
                        params.setKnowledgeSystem(Lists.newArrayList(knowledgeSystemResp.getKnowledgeSystemId()));
                        return syncProjectBiz.getPxbProjects(params, true).getTotal() != 0;
                })
                .collect(Collectors.toList());
    }

    @Override
    public List<PxbOrganizationResp> getPxbOrganizations(ApiRequestParam<HashMap, HashMap> request) {
        String keyword = MapUtils.getString(request.getBodyParams(), "keyword");
        String type = MapUtils.getString(request.getBodyParams(), "type");
        String searchRange = MapUtils.getString(request.getBodyParams(), "search_range");
        String rootId = "all".equals(searchRange) ? null : request.getUser().getOrganizationId();
        boolean isManger = request.getUser().isManager();

        if (StringUtils.equals("1", type)) {
            return syncOrgBiz.getPxbOrganizationsTree(rootId, isManger);
        } else {
            return syncOrgBiz.getPxbOrganizations(rootId, isManger, keyword);
        }
    }

    @Override
    public List<PxbPositionResp> getPxbPositions(ApiRequestParam<String, HashMap> request) {
        return syncUserInfoBiz.getPxbPositions();
    }

    @Override
    public PageResultForWD<PxbProjectsResp> getPxbProjects(ApiRequestParam<GetPxbProjectsReq, Map<String, Object>> request) {
        GetPxbProjectsReq params = request.getBodyParams();
        if (Strings.isNotBlank(params.getMemberName()) || Strings.isNotBlank(params.getMemberCode())) {
            Map<String, Object> userInfo = syncUserInfoBiz.getUserInfo(
                params.getMemberName(),
                params.getMemberCode()
            );
            if (Objects.isNull(userInfo)) {
                return PageResultForWD.success(0, params.getPage(), params.getPageSize(), Lists.newArrayList());
            }
            params.setOrganizationPath(MapUtils.getString(userInfo, PxbUserInfo.DEFAULT_ORGANIZATION_PATH));
            params.setMemberCode(MapUtils.getString(userInfo, PxbUserInfo.CODE));
            params.setPositionIds(Lists.newArrayList(MapUtils.getString(userInfo, PxbUserInfo.POSITION_ID)));
            return syncProjectBiz.getSelfPxbProjects(params, true);

        }
        return syncProjectBiz.getPxbProjects(params, true);
    }

    @Override
    public PageResultForWD<PxbProjectsResp> getCurrentPxbProjects(ApiRequestParam<GetPxbProjectsReq, Map<String, Object>> request) {
        GetPxbProjectsReq params = request.getBodyParams();
        UserVo user = request.getUser();
        if (SearchProjectRangeEnum.ORGANIZATION_PROJECT == params.getSearchProjectRange()) {
            if (Strings.isBlank(params.getOrganizationPath())) {
                if (Strings.isNotBlank(params.getMemberName()) || Strings.isNotBlank(params.getMemberCode())) {
                    Map<String, Object> userInfo = syncUserInfoBiz.getUserInfo(
                        params.getMemberName(),
                        params.getMemberCode()
                    );
                    if (Objects.isNull(userInfo)) {
                        return PageResultForWD.success(0, params.getPage(), params.getPageSize(), Lists.newArrayList());
                    }
                    Map<String, String> info = syncUserInfoBiz.getOrganizationPathAndPosition(userInfo, params.getOrganizationPath(), params.isManager());
                    if (Objects.isNull(MapUtils.getString(info, Organization.PATH))) {
                        return PageResultForWD.success(0, params.getPage(), params.getPageSize(), Lists.newArrayList());
                    }
                    String organizationPath = MapUtils.getString(info, Organization.PATH);
                    params.setOrganizationPath(organizationPath.contains(user.getOrganizationPath()) ? null : user.getOrganizationPath());
                } else {
                    params.setOrganizationPath(user.getOrganizationPath());
                }
            }
            if (!Boolean.TRUE.equals(params.getIsCurrentMonth()) && (Objects.isNull(params.getKnowledgeSystem()) || params.getKnowledgeSystem().isEmpty())) {
                params.setKnowledgeSystem(syncKnowledgeSystemBiz.getOrganizationKnowledgeSystemIds(user.getOrganizationPath(), user.isManager()));
                if (params.getKnowledgeSystem().isEmpty()) {
                    return PageResultForWD.success(0, params.getPage(), params.getPageSize(), Lists.newArrayList());
                }
            }
            params.setManager(user.isManager());
            return syncProjectMemberBiz.getOrganizationPxbProjects(params);
        }
        if (SearchProjectRangeEnum.SELF_PROJECT == params.getSearchProjectRange()) {
            Map<String, Object> userInfo = syncUserInfoBiz.getUserInfo(user.getId());
            if (Objects.isNull(userInfo)) {
                return PageResultForWD.success(0, params.getPage(), params.getPageSize(), Lists.newArrayList());
            }
            if (Objects.isNull(params.getKnowledgeSystem()) || params.getKnowledgeSystem().isEmpty()) {
                List<KnowledgeSystemResp> knowledgeSystemResp = syncKnowledgeSystemBiz.getSelfPxbKnowledgeSystem(userInfo);
                params.setKnowledgeSystem(knowledgeSystemResp.stream().map(KnowledgeSystemResp::getKnowledgeSystemId).collect(Collectors.toList()));
            }
            if (!Boolean.TRUE.equals(params.getIsCurrentMonth()) && params.getKnowledgeSystem().isEmpty()) {
                return PageResultForWD.success(0, params.getPage(), params.getPageSize(), Lists.newArrayList());
            }
            params.setOrganizationPath(MapUtils.getString(userInfo, PxbUserInfo.DEFAULT_ORGANIZATION_PATH));
            params.setMemberCode(MapUtils.getString(userInfo, PxbUserInfo.CODE));
            params.setPositionIds(Lists.newArrayList(MapUtils.getString(userInfo, PxbUserInfo.POSITION_ID)));
            Map<Long, String> systemWithEndingTime = syncKnowledgeSystemBiz.getSelfPxbKnowledgeSystemWithEndingTime(userInfo);
            return syncProjectMemberBiz.getSelfPxbProjects(params, systemWithEndingTime);
        }
        return PageResultForWD.success(0, params.getPage(), params.getPageSize(), Lists.newArrayList());
    }

    @Override
    public PageResultForWD<PxbProjectMemberResp> getPxbProjectMember(ApiRequestParam<GetPxbProjectMemberReq, Map<String, Object>> request) {
        GetPxbProjectMemberReq params = request.getBodyParams();
        return syncUserInfoBiz.getPxbProjectMember(params, null);
    }

    @Override
    public PageResultForWD<PxbProjectMemberResp> getCurrentPxbProjectMember(ApiRequestParam<GetPxbProjectMemberReq, Map<String, Object>> request) {
        GetPxbProjectMemberReq params = request.getBodyParams();
        params.setManager(request.getUser().isManager());
        UserVo user = request.getUser();
        return syncUserInfoBiz.getPxbProjectMember(params, user.getOrganizationPath());
    }

    @Override
    public Map<String, Object> updateProjectInfo(ApiRequestParam<ProjectInfoUpdateRequest, HashMap> request) {
        if (Strings.isNotBlank(request.getBodyParams().getLearningMessage()) && request.getBodyParams().getLearningMessage().length() > 50) {
            throw new CrmSideException("学习寄语超出50个字符，请重新输入");
        }
        return syncProjectBiz.updateProjectInfo(request.getBodyParams());
    }

    private void syncInfoAndSave(PxbApis<Map<String, Object>> api, Date begin, Date end, Consumer<Map<String, Object>> consumer) {
        Map<String, Long> body = new HashMap<String, Long>() {{
            put("begin_time", begin.getTime() / 1000);
            put("end_time", end.getTime() / 1000);
            put("start", 1L);
            put("count", 100L);
            put("is_all", 1L);
        }};
        while (true) {
            log.info(String.format("同步数据进行中，api为%s, 参数为%s", api.getPath(), JSON.toJSONString(body)));
            Response<Map<String, Object>> response = pxbThirdApi.execute(api.addBody(body));
            Map<String, Object> data = response.getData();
            Object listObj = MapUtils.getObject(data, "list");
            if (!(listObj instanceof List)) {
                return;
            }
            List<Map<String, Object>> list = (List<Map<String, Object>>) listObj;

            List<Map<String, Object>> xhList = formateList(list);
            xhList.forEach(consumer);
            long next = MapUtils.getLongValue(data, "next");
            if (Objects.equals(next, 0L)) {
                break;
            }
            body.put("start", next);
        }
    }

    private void syncProjectMember(Date begin) {
        List<Long> pxbProjectIds = syncProjectBiz.findPxbProjectIds(begin);
        PxbApis<Map<String, Object>> api = PxbApis.PROJECTS_STUDENT;
        Map<String, Object> body = new HashMap<String, Object>() {{
            put("last_time", Objects.nonNull(begin) ? begin.getTime() / 1000 : -1L);
            put("start", 0L);
            put("count", 200L);
            put("is_all", 0L);
        }};
        List<List<Long>> partition = Lists.partition(pxbProjectIds, 10);
        for (List<Long> part: partition) {
            body.put("project_ids", part.stream().map(String::valueOf).collect(Collectors.joining(",")));
            while (true) {
                log.info(String.format("同步数据进行中，api为%s, 参数为%s", api.getPath(), JSON.toJSONString(body)));
                Response<Map<String, Object>> response = pxbThirdApi.execute(api.addBody(body));
                Map<String, Object> data = response.getData();
                Object listObj = MapUtils.getObject(data, "list");
                if (!(listObj instanceof List)) {
                    return;
                }
                List<Map<String, Object>> list = (List<Map<String, Object>>) listObj;

                List<Map<String, Object>> xhList = formateList(list);
                xhList.forEach(syncProjectMemberBiz::saveAndUpdateProjectMember);
                long next = MapUtils.getLongValue(data, "next");
                if (Objects.equals(next, 0L)) {
                    break;
                }
                body.put("start", next);
            }
        }
    }

    public PageResultForWD<PxbProjectMemberResp> getPxbProjectMember1(
        GetPxbProjectMemberReq params,
        String organizationPath) {
        Map<String, Object> pxbProject = pxbProjectDao.filter(Criteria.where(PxbProject.PXB_ID).is(params.getProjectId())).fetchOne();
        List<String> positionIds = getPositionIds(pxbProject);
        List<String> organizationCodeWithSubOrgs = getOrganizationCodeWithSubOrgs(pxbProject);
        List<String> organizationCodes = getOrganizationCodes(pxbProject);
        Criteria criteria = Criteria.where(PxbUserInfo.DELETED).is(0);
        if (Strings.isNotBlank(organizationPath)) {
            List<Criteria> list = Lists.newArrayList();
            List<String> codes = Lists.newArrayList(organizationPath.split(","));
            positionIds.clear();
            if (params.isManager() && organizationCodes.contains(codes.get(codes.size() - 1))) {
                list.add(Criteria.where(PxbUserInfo.DEFAULT_ORGANIZATION_PATH).is(organizationPath));
            }
            if (organizationCodeWithSubOrgs.stream().anyMatch(codes::contains)) {
                if (params.isManager()) {
                    list.add(Criteria.where(PxbUserInfo.DEFAULT_ORGANIZATION_PATH).is(organizationPath));
                }
                list.add(Criteria.where(PxbUserInfo.DEFAULT_ORGANIZATION_PATH).like(organizationPath + ",%"));
            }
            organizationCodeWithSubOrgs.clear();
            organizationCodes.clear();
            if (!list.isEmpty()) {
                criteria.withAnd(Criteria.or(list));
            }
        }
        List<Criteria> list = Lists.newArrayList();
        if (!positionIds.isEmpty()) {
            list.add(Criteria.where(PxbUserInfo.POSITION_ID).in(positionIds));
        }
        if (!organizationCodeWithSubOrgs.isEmpty()) {
            for(String code: organizationCodeWithSubOrgs) {
                list.add(Criteria.where(PxbUserInfo.DEFAULT_ORGANIZATION_PATH).like("%" + code + ",%"));
                list.add(Criteria.where(PxbUserInfo.DEFAULT_ORGANIZATION_PATH).like("%" + code));
            }
        }
        if (!organizationCodes.isEmpty()) {
            for (String code: organizationCodes) {
                list.add(Criteria.where(PxbUserInfo.DEFAULT_ORGANIZATION_PATH).like("%" + code));
            }
        }
        if (!list.isEmpty()) {
            criteria.withAnd(Criteria.or(list));
        }
        if (Strings.isNotBlank(params.getMemberName())) {
            criteria.with(PxbUserInfo.CHINESE_NAME).is(params.getMemberName());
        }
        if (Strings.isNotBlank(params.getMemberCode())) {
            criteria.with(PxbUserInfo.CODE).is(params.getMemberCode());
        }
        Filter<Map<String, Object>> filter = pxbUserInfoDao.filter(criteria);
        long total = filter.count();
        PageInfo pageInfo = new PageInfo(params.getPage(), params.getPageSize());
        PageInfoUtil.pagination(pageInfo, filter);
        Iterator<Map<String, Object>> data = filter.fetchAll();
        List<PxbProjectMemberResp> result =
            Lists.newArrayList(data)
                .stream()
                .map(value -> {
                    Map<String, Object> member =
                        pxbProjectMemberDao
                            .filter(Criteria.where(PxbProjectMember.PROJECT_ID).is(params.getProjectId()).with(PxbProjectMember.CODE).is(MapUtils.getString(value, PxbUserInfo.CODE)))
                            .fetchOne();
                    Map organization = organizationService.getSimpleOrganization(
                        MapsUtil.hashMap(Organization.PATH, MapUtils.getString(value, PxbUserInfo.DEFAULT_ORGANIZATION_PATH)));
                    Map positionDetail = positionService.getPositionDetail(MapUtils.getString(
                        value,
                        PxbUserInfo.POSITION_ID
                    ));
                    Integer status = Objects.isNull(member) ? -1 : MapUtils.getInteger(member, PxbProjectMember.STATUS);
                    return
                        PxbProjectMemberResp
                            .builder()
                            .userId(MapUtils.getString(value, PxbUserInfo.USER_ID))
                            .organizationName(MapUtils.getString(organization, Organization.NAME))
                            .position(MapUtils.getString(positionDetail, Position.NAME))
                            .memberName(MapUtils.getString(value, PxbUserInfo.CHINESE_NAME))
                            .memberCode(MapUtils.getString(value, PxbUserInfo.CODE))
                            .status(status)
                            .build();
                })
                .collect(Collectors.toList());
        return PageResultForWD.success(total, params.getPage(), params.getPageSize(), result);
    }
}
