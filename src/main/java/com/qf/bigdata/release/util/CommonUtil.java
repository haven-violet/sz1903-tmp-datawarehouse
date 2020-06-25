package com.qf.bigdata.release.util;

import com.qf.bigdata.release.enums.AgeRangerEnum;

public class CommonUtil {

    /**
     * 年龄段
     * @param age
     * @return
     */
    public static String getAgeRange(String age) {
        String ageRange = "";
        try {
            int ageInt = Integer.valueOf(age);

            if(AgeRangerEnum.AGE_18.getBegin() <= ageInt && ageInt <= AgeRangerEnum.AGE_18.getEnd()){
                ageRange = AgeRangerEnum.AGE_18.getCode();

            }else if(AgeRangerEnum.AGE_18_25.getBegin() <= ageInt && ageInt <= AgeRangerEnum.AGE_18_25.getEnd()){
                ageRange = AgeRangerEnum.AGE_18_25.getCode();

            }else if(AgeRangerEnum.AGE_26_35.getBegin() <= ageInt && ageInt <= AgeRangerEnum.AGE_26_35.getEnd()){
                ageRange = AgeRangerEnum.AGE_26_35.getCode();

            }else if(AgeRangerEnum.AGE_36_45.getBegin() <= ageInt && ageInt <= AgeRangerEnum.AGE_36_45.getEnd()){
                ageRange = AgeRangerEnum.AGE_36_45.getCode();

            }else if(AgeRangerEnum.AGE_45.getBegin() <= ageInt && ageInt <= AgeRangerEnum.AGE_45.getEnd()){
                ageRange = AgeRangerEnum.AGE_45.getCode();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ageRange;
    }
}