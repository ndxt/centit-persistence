package com.centit.support.database.orm;

/**
 * Created by codefan on 17-8-29.
 */
public enum GeneratorTime {
    // 在新建的时候生成
    NEW,
    //在修改式生成
    UPDATE,
    //在查询时生成
    READ,
    // 在new 和 update 的时候都保存
    NEW_UPDATE,
    //任何时候都生成 READ_NEW_UPDATE
    ALWAYS;

    GeneratorTime() {
    }

    public boolean matchTime(GeneratorTime other) {
        return this.equals(other) ||
            this.equals(ALWAYS) || other.equals(ALWAYS) ||
            (this.equals(NEW_UPDATE) && (other.equals(UPDATE) || other.equals(NEW))) ||
            (other.equals(NEW_UPDATE) && (this.equals(UPDATE) || this.equals(NEW)));
    }
}
