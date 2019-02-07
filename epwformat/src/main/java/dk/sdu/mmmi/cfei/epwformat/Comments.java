package dk.sdu.mmmi.cfei.epwformat;

import java.util.Objects;

class Comments {

    public Comments(int number, String comment) {
        this.number = number;
        this.comment = comment;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 61 * hash + this.number;
        hash = 61 * hash + Objects.hashCode(this.comment);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Comments other = (Comments) obj;
        if (this.number != other.number) {
            return false;
        }
        if (!Objects.equals(this.comment, other.comment)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Comments{" + "number=" + number + ", comment=" + comment + '}';
    }

    public final int number;
    public final String comment;
}
