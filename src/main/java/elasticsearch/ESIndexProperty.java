package elasticsearch;
/**
 * @Title:
 *   ES索引属性对应
 * @description:
 * 
 * @Company: ultrapower.com
 * @author lnj2050 
 * @create time：2016年5月12日  下午2:02:22
 * @version 1.0
 */
public class ESIndexProperty {
    
    private String name;
    
    private String type;
    
    private String index;
    
    /** 是否为维度属性 */
    private boolean isdim = false;

    public boolean isIsdim() {
		return isdim;
	}

	public void setIsdim(boolean isdim) {
		this.isdim = isdim;
	}

	public String getResClassTitle() {
		return resClassTitle;
	}

	public void setResClassTitle(String resClassTitle) {
		this.resClassTitle = resClassTitle;
	}

	public String getResDisplayProperty() {
		return resDisplayProperty;
	}

	public void setResDisplayProperty(String resDisplayProperty) {
		this.resDisplayProperty = resDisplayProperty;
	}

	public String getResProperty() {
		return resProperty;
	}

	public void setResProperty(String resProperty) {
		this.resProperty = resProperty;
	}

	public String getResPropertyFrom() {
		return resPropertyFrom;
	}

	public void setResPropertyFrom(String resPropertyFrom) {
		this.resPropertyFrom = resPropertyFrom;
	}

	/** 维度资源类型 */
    private String resClassTitle = null;

    /** 界面显示时使用的资源属性名称 */
    private String resDisplayProperty = null;

    /** 资源属性名称 */
    private String resProperty = null;

    /** 资源属性来源，值为node或instance */
    private String resPropertyFrom = null;
    
    public ESIndexProperty() {
    }

    public ESIndexProperty(String name, String type, String index) {
        this.name = name;
        this.type = type;
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ESIndexProperty [name=");
        builder.append(name);
        builder.append(", type=");
        builder.append(type);
        builder.append(", index=");
        builder.append(index);
        builder.append("]");
        return builder.toString();
    }
}

