import { Chip } from "../chip/chip.js";

export interface ChipGroupOption {
  value: string;
  label: string;
}

interface ChipGroupBaseProps {
  options: ChipGroupOption[];
  className?: string;
}

interface ChipGroupSingleProps extends ChipGroupBaseProps {
  multiple?: false;
  value: string;
  onChange: (value: string) => void;
}

interface ChipGroupMultipleProps extends ChipGroupBaseProps {
  multiple: true;
  value: string[];
  onChange: (value: string[]) => void;
}

export type ChipGroupProps = ChipGroupSingleProps | ChipGroupMultipleProps;

export function ChipGroup(props: ChipGroupProps) {
  const { options, className = "" } = props;

  return (
    <div className={`flex items-center gap-2 ${className}`}>
      {options.map((option) => {
        const isActive = props.multiple
          ? props.value.includes(option.value)
          : props.value === option.value;

        const handleClick = () => {
          if (props.multiple) {
            const next = props.value.includes(option.value)
              ? props.value.filter((v) => v !== option.value)
              : [...props.value, option.value];
            props.onChange(next);
          } else {
            props.onChange(option.value);
          }
        };

        return (
          <Chip key={option.value} active={isActive} onClick={handleClick}>
            {option.label}
          </Chip>
        );
      })}
    </div>
  );
}
